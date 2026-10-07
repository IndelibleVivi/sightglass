// Sightglass local voice transcription helper.
//
// Reads one private audio input (raw 16 kHz mono s16le PCM from the Sightglass decoder,
// or any AVAudioFile-readable container for local diagnostics), transcribes it with
// Apple's on-device SpeechAnalyzer + SpeechTranscriber, and prints exactly one JSON line
// on stdout. Only final results are reported; volatile results never reach stdout.
//
// The transcribe path never downloads or installs speech assets, never opens a
// microphone, and never translates or rewrites what it heard: if the requested locale
// has no installed asset it exits with code 2 and one bounded stderr line instead of
// fetching anything.  The separate operator-only `--install-assets` mode is the single
// deliberate exception: run by hand, it asks Apple's AssetInventory to download the
// requested locale's on-device speech asset and reports the resulting status.
//
// Exit codes: 0 success, 2 model/locale unavailable, 3 transcription failure, 4 usage/input.

import AVFAudio
import CoreMedia
import Foundation
import Speech

let helperVersion = "2"
let maxSegments = 512
let maxTextCharacters = 200_000

struct HelperError: Error {
    let code: Int32
    let reason: String
}

func fail(_ code: Int32, _ reason: String) -> Never {
    writeStderr(["error": reason, "exit_code": Int(code)])
    exit(code)
}

func writeStderr(_ payload: [String: Any]) {
    let data = (try? JSONSerialization.data(withJSONObject: payload, options: [.sortedKeys]))
        ?? Data("{\"error\":\"unknown\"}".utf8)
    var line = data
    line.append(0x0a)
    FileHandle.standardError.write(line)
}

func emit(_ payload: [String: Any]) -> Never {
    guard let data = try? JSONSerialization.data(withJSONObject: payload, options: [.sortedKeys])
    else {
        fail(3, "result_serialization_failed")
    }
    var line = data
    line.append(0x0a)
    FileHandle.standardOutput.write(line)
    exit(0)
}

struct Options {
    let path: String?
    let isRawPCM: Bool
    let localeIdentifier: String
    let installAssets: Bool
}

func parseArguments(_ arguments: [String]) -> Options {
    var path: String?
    var localeIdentifier = "zh-CN"
    var isRawPCM = false
    var installAssets = false
    var index = 1
    while index < arguments.count {
        let value = arguments[index]
        switch value {
        case "--version":
            FileHandle.standardOutput.write(
                Data(("sightglass-transcribe " + helperVersion + "\n").utf8))
            exit(0)
        case "--install-assets":
            installAssets = true
        case "--pcm", "--audio":
            index += 1
            guard index < arguments.count else { fail(4, "missing_input_path") }
            path = arguments[index]
            isRawPCM = value == "--pcm"
        case "--locale":
            index += 1
            guard index < arguments.count else { fail(4, "missing_locale") }
            localeIdentifier = arguments[index]
        default:
            fail(4, "unknown_argument")
        }
        index += 1
    }
    if !installAssets, path == nil { fail(4, "missing_input_path") }
    return Options(
        path: path, isRawPCM: isRawPCM, localeIdentifier: localeIdentifier,
        installAssets: installAssets)
}

func installAssets(_ localeIdentifier: String) async {
    let requested = Locale(identifier: localeIdentifier)
    guard let target = await SpeechTranscriber.supportedLocale(equivalentTo: requested) else {
        fail(2, "unsupported_locale")
    }
    let identifier = target.identifier(.bcp47)
    let alreadyInstalled = await SpeechTranscriber.installedLocales
        .contains(where: { $0.identifier(.bcp47) == identifier })
    var action = "already_installed"
    if !alreadyInstalled {
        writeStderr(["install": "started", "locale": identifier])
        do {
            let transcriber = SpeechTranscriber(locale: target, preset: .transcription)
            guard let request = try await AssetInventory.assetInstallationRequest(supporting: [transcriber])
            else {
                fail(3, "install_request_unavailable")
            }
            try await request.downloadAndInstall()
            action = "installed"
        } catch {
            fail(3, "install_failed")
        }
    }
    let nowInstalled = await SpeechTranscriber.installedLocales
        .contains(where: { $0.identifier(.bcp47) == identifier })
    guard nowInstalled else { fail(3, "install_not_verified") }
    emit([
        "schema": "sightglass.voice-asset-install.v1",
        "helper": "sightglass-transcribe",
        "helper_version": helperVersion,
        "action": action,
        "locale": identifier,
        "asset_status": "installed",
    ])
}

func orderedSegments(_ transcriber: SpeechTranscriber) async throws -> [[String: Any]] {
    var segments: [[String: Any]] = []
    for try await result in transcriber.results {
        guard result.isFinal else { continue }
        let text = String(result.text.characters)
        guard !text.isEmpty else { continue }
        guard segments.count < maxSegments else { continue }
        let startMs = Int((result.range.start.seconds * 1000).rounded())
        let endMs = Int((CMTimeRangeGetEnd(result.range).seconds * 1000).rounded())
        segments.append(["start_ms": startMs, "end_ms": endMs, "text": text])
    }
    return segments
}

func joinedText(_ segments: [[String: Any]]) -> String {
    var pieces: [String] = []
    var total = 0
    for segment in segments {
        guard let text = segment["text"] as? String else { continue }
        total += text.count
        if total > maxTextCharacters { break }
        pieces.append(text)
    }
    return pieces.joined()
}

func resolvedAssetStatus(_ localeIdentifier: String) async -> (String, Locale?) {
    let requested = Locale(identifier: localeIdentifier)
    guard let target = await SpeechTranscriber.supportedLocale(equivalentTo: requested) else {
        return ("unsupported_locale", nil)
    }
    let installed = await SpeechTranscriber.installedLocales
    let identifier = target.identifier(.bcp47)
    if installed.contains(where: { $0.identifier(.bcp47) == identifier }) {
        return ("installed", target)
    }
    return ("not_installed", target)
}

func transcribeRawPCM(_ transcriber: SpeechTranscriber, url: URL) async throws -> [[String: Any]] {
    guard
        let format = AVAudioFormat(
            commonFormat: .pcmFormatInt16, sampleRate: 16_000, channels: 1, interleaved: true)
    else {
        throw HelperError(code: 4, reason: "pcm_format_unavailable")
    }
    let data = try Data(contentsOf: url)
    let frameSize = MemoryLayout<Int16>.size
    guard data.count % frameSize == 0 else {
        throw HelperError(code: 4, reason: "pcm_not_16bit_aligned")
    }
    let frameCount = data.count / frameSize
    guard frameCount > 0 else {
        throw HelperError(code: 4, reason: "pcm_empty")
    }
    let analyzer = SpeechAnalyzer(modules: [transcriber])
    try await analyzer.prepareToAnalyze(in: format)
    let (stream, continuation) = AsyncStream<AnalyzerInput>.makeStream()
    let collector = Task { try await orderedSegments(transcriber) }
    try await analyzer.start(inputSequence: stream)
    let chunkFrames = 16_000
    var offset = 0
    while offset < frameCount {
        let take = min(chunkFrames, frameCount - offset)
        guard
            let buffer = AVAudioPCMBuffer(
                pcmFormat: format, frameCapacity: AVAudioFrameCount(take))
        else {
            throw HelperError(code: 4, reason: "pcm_buffer_unavailable")
        }
        buffer.frameLength = AVAudioFrameCount(take)
        data.withUnsafeBytes { raw -> Void in
            guard let base = raw.baseAddress else { return }
            memcpy(
                buffer.int16ChannelData![0],
                base.advanced(by: offset * frameSize),
                take * frameSize)
        }
        continuation.yield(AnalyzerInput(buffer: buffer))
        offset += take
    }
    continuation.finish()
    try await analyzer.finalizeAndFinishThroughEndOfInput()
    return try await collector.value
}

func transcribeAudioFile(
    _ transcriber: SpeechTranscriber, url: URL
) async throws -> [[String: Any]] {
    let file = try AVAudioFile(forReading: url)
    // The convenience initializer already starts analyzing this file, so there must be
    // no second input sequence: this path only consumes final results.
    let analyzer = try await SpeechAnalyzer(
        inputAudioFile: file, modules: [transcriber], finishAfterFile: true)
    let segments = try await orderedSegments(transcriber)
    withExtendedLifetime(analyzer) {}
    return segments
}

@main
struct SightglassTranscribe {
    static func main() async {
        let options = parseArguments(CommandLine.arguments)
        if options.installAssets {
            await installAssets(options.localeIdentifier)
        }
        let (assetStatus, target) = await resolvedAssetStatus(options.localeIdentifier)
        guard assetStatus == "installed", let locale = target else {
            fail(2, assetStatus)
        }
        let transcriber = SpeechTranscriber(
            locale: locale,
            transcriptionOptions: [],
            reportingOptions: [.volatileResults],
            attributeOptions: [])
        guard let inputPath = options.path else { fail(4, "missing_input_path") }
        let url = URL(fileURLWithPath: inputPath)
        do {
            let segments =
                options.isRawPCM
                ? try await transcribeRawPCM(transcriber, url: url)
                : try await transcribeAudioFile(transcriber, url: url)
            emit([
                "schema": "sightglass.voice-transcript.v1",
                "helper": "sightglass-transcribe",
                "helper_version": helperVersion,
                "backend": "SpeechAnalyzer+SpeechTranscriber",
                "locale": locale.identifier(.bcp47),
                "asset_status": assetStatus,
                "model": [
                    "identifier": "unknown",
                    "asset_status": assetStatus,
                    "resolved_locale": locale.identifier(.bcp47),
                ],
                "os_version": ProcessInfo.processInfo.operatingSystemVersionString,
                "volatile_excluded": true,
                "text": joinedText(segments),
                "segments": segments,
            ])
        } catch let error as HelperError {
            fail(error.code, error.reason)
        } catch {
            fail(3, "transcription_failed")
        }
    }
}

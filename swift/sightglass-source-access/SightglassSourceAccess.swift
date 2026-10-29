import AppKit
import CryptoKit
import Darwin
import Foundation

private let protocolSchema = "sightglass.source-access.v3"
private let helperVersion = "3"
private let maximumRequestBytes = 24 * 1024
private let maximumResponseBytes = 768 * 1024
private let capabilitySchema = "sightglass.source-access.capability.v3"

private struct HelperError: Error {
    let code: String
}

private struct ItemIdentity: Codable, Equatable {
    let path: String
    let inode: UInt64
    let device: UInt64
    let kind: String
}

private struct Request: Codable {
    let schema: String
    let mode: String
    let nonce: String
    let expectedAccount: ItemIdentity?
    let expectedSharedConfig: ItemIdentity?
    let expectedSharedConfigMeta: ItemIdentity?
    let includeSharedConfig: Bool
    let expectedHelperDigest: String
    let receiverProof: String?
    let expectedReceiverVerifier: String
}

private struct StoredItem: Codable {
    let identity: ItemIdentity
    let bookmark: String
    let stale: Bool
}

private struct StoredCapability: Codable {
    let schema: String
    let account: StoredItem
    let sharedConfig: StoredItem?
    let sharedConfigMeta: StoredItem?
    let receiverVerifier: String
}

private struct Response: Codable {
    let schema: String
    let ok: Bool
    let mode: String
    let nonce: String?
    let code: String?
    let helperVersion: String?
    let helperDigest: String?
    let capabilityPresent: Bool?
    let grantsAccess: Bool
    let account: ItemIdentity?
    let accountDigest: String?
    let sharedConfig: ItemIdentity?
    let sharedConfigBookmark: String?
    let sharedConfigDigest: String?
    let sharedConfigMeta: ItemIdentity?
    let sharedConfigMetaBookmark: String?
    let sharedConfigMetaDigest: String?
    let accountBookmark: String?
    let receiverProofAccepted: Bool?
    let receiverVerifier: String?
    let entitlementReady: Bool?
    let containerReady: Bool?
}

private func receiverVerifier(_ proof: String) -> String {
    sha256(Data(
        "sightglass.source-access.receiver-verifier.v1\u{0}\(proof)".utf8
    ))
}

private func proofMatches(_ proof: String?, verifier: String) -> Bool {
    guard let proof,
          proof.range(of: "^[a-f0-9]{64}$", options: .regularExpression) != nil
    else {
        return false
    }
    let actual = receiverVerifier(proof)
    guard actual.utf8.count == verifier.utf8.count else {
        return false
    }
    return zip(actual.utf8, verifier.utf8).reduce(UInt8(0)) {
        $0 | ($1.0 ^ $1.1)
    } == 0
}

private func encodeLine<T: Encodable>(_ value: T) throws -> Data {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
    let payload = try encoder.encode(value)
    guard payload.count <= maximumResponseBytes else {
        throw HelperError(code: "source_access_response_too_large")
    }
    return payload + Data([0x0A])
}

private func emit(_ response: Response) throws {
    FileHandle.standardOutput.write(try encodeLine(response))
}

private func errorResponse(_ code: String, request: Request? = nil) -> Response {
    Response(
        schema: protocolSchema,
        ok: false,
        mode: request?.mode ?? "check",
        nonce: request?.nonce,
        code: code,
        helperVersion: helperVersion,
        helperDigest: request?.expectedHelperDigest,
        capabilityPresent: nil,
        grantsAccess: false,
        account: nil,
        accountDigest: nil,
        sharedConfig: nil,
        sharedConfigBookmark: nil,
        sharedConfigDigest: nil,
        sharedConfigMeta: nil,
        sharedConfigMetaBookmark: nil,
        sharedConfigMetaDigest: nil,
        accountBookmark: nil,
        receiverProofAccepted: false,
        receiverVerifier: nil,
        entitlementReady: nil,
        containerReady: nil
    )
}

private func readRequest() throws -> Request {
    var data = Data()
    var terminated = false
    while data.count <= maximumRequestBytes {
        let byte = FileHandle.standardInput.readData(ofLength: 1)
        if byte.isEmpty { break }
        if byte[0] == 0x0A {
            terminated = true
            break
        }
        data.append(byte)
    }
    guard terminated, !data.isEmpty, data.count <= maximumRequestBytes else {
        throw HelperError(code: "source_access_request_size_invalid")
    }
    let request: Request
    do {
        request = try JSONDecoder().decode(Request.self, from: data)
    } catch {
        throw HelperError(code: "source_access_request_invalid")
    }
    guard request.schema == protocolSchema,
          ["check", "authorize", "lease"].contains(request.mode),
          !request.nonce.isEmpty,
          request.nonce.utf8.count <= 128,
          request.expectedHelperDigest.range(
            of: "^[a-f0-9]{64}$", options: .regularExpression
          ) != nil,
          request.receiverProof?.range(
            of: "^[a-f0-9]{64}$", options: .regularExpression
          ) != nil || (request.mode == "check" && request.receiverProof == nil),
          request.expectedReceiverVerifier.range(
            of: "^[a-f0-9]{64}$", options: .regularExpression
          ) != nil
    else {
        throw HelperError(code: "source_access_request_invalid")
    }
    if request.mode == "check" {
        guard request.expectedAccount == nil,
              request.expectedSharedConfig == nil,
              request.expectedSharedConfigMeta == nil,
              !request.includeSharedConfig,
              request.receiverProof == nil
        else {
            throw HelperError(code: "source_access_request_invalid")
        }
    } else {
        guard request.expectedAccount?.kind == "account_directory" else {
            throw HelperError(code: "source_access_request_invalid")
        }
        if request.includeSharedConfig != (
            request.expectedSharedConfig != nil &&
            request.expectedSharedConfigMeta != nil
        ) {
            throw HelperError(code: "source_access_request_invalid")
        }
        if request.expectedSharedConfig?.kind != nil &&
            request.expectedSharedConfig?.kind != "shared_config_file"
        {
            throw HelperError(code: "source_access_request_invalid")
        }
        if request.expectedSharedConfigMeta?.kind != nil &&
            request.expectedSharedConfigMeta?.kind != "shared_config_meta_file"
        {
            throw HelperError(code: "source_access_request_invalid")
        }
    }
    return request
}

private func sha256(_ data: Data) -> String {
    SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
}

private func identityDigest(_ item: ItemIdentity) -> String {
    sha256(Data(
        "sightglass.source-access.item.v3\u{0}\(item.kind)\u{0}\(item.path)\u{0}\(item.device)\u{0}\(item.inode)".utf8
    ))
}

private func identity(
    _ url: URL, kind: String, requireAccountLayout: Bool
) throws -> ItemIdentity {
    let values = try url.resourceValues(forKeys: [
        .isDirectoryKey, .isRegularFileKey, .isSymbolicLinkKey
    ])
    guard values.isSymbolicLink != true else {
        throw HelperError(code: "source_access_root_invalid")
    }
    if kind == "account_directory" {
        guard values.isDirectory == true else {
            throw HelperError(code: "source_access_root_invalid")
        }
    } else {
        guard values.isRegularFile == true else {
            throw HelperError(code: "source_access_root_invalid")
        }
    }
    let path = url.standardizedFileURL.path
    let attributes = try FileManager.default.attributesOfItem(atPath: path)
    guard let inode = (attributes[.systemFileNumber] as? NSNumber)?.uint64Value,
          let device = (attributes[.systemNumber] as? NSNumber)?.uint64Value
    else {
        throw HelperError(code: "source_access_root_invalid")
    }
    if requireAccountLayout {
        let databaseRoot = url.appendingPathComponent("db_storage", isDirectory: true)
        guard databaseRoot.lastPathComponent == "db_storage" else {
            throw HelperError(code: "source_access_account_layout_invalid")
        }
        for relative in ["contact/contact.db", "session/session.db"] {
            let values = try databaseRoot.appendingPathComponent(relative)
                .resourceValues(forKeys: [.isRegularFileKey, .isSymbolicLinkKey])
            guard values.isRegularFile == true, values.isSymbolicLink != true else {
                throw HelperError(code: "source_access_account_layout_invalid")
            }
        }
    }
    return ItemIdentity(path: path, inode: inode, device: device, kind: kind)
}

private func containerDirectory() throws -> URL {
    guard let applicationSupport = FileManager.default.urls(
        for: .applicationSupportDirectory, in: .userDomainMask
    ).first else {
        throw HelperError(code: "source_access_container_unavailable")
    }
    let directory = applicationSupport.appendingPathComponent(
        "Sightglass Source Access", isDirectory: true
    )
    try FileManager.default.createDirectory(
        at: directory, withIntermediateDirectories: true,
        attributes: [.posixPermissions: 0o700]
    )
    try FileManager.default.setAttributes(
        [.posixPermissions: 0o700], ofItemAtPath: directory.path
    )
    let values = try directory.resourceValues(forKeys: [.isDirectoryKey, .isSymbolicLinkKey])
    guard values.isDirectory == true, values.isSymbolicLink != true else {
        throw HelperError(code: "source_access_container_unsafe")
    }
    return directory
}

private func capabilityPath() throws -> URL {
    try containerDirectory().appendingPathComponent("capability.json", isDirectory: false)
}

private func loadCapability() throws -> StoredCapability {
    let path = try capabilityPath()
    let attributes: [FileAttributeKey: Any]
    do {
        let values = try path.resourceValues(forKeys: [.isRegularFileKey, .isSymbolicLinkKey])
        guard values.isRegularFile == true, values.isSymbolicLink != true else {
            throw HelperError(code: "source_access_capability_invalid")
        }
        attributes = try FileManager.default.attributesOfItem(atPath: path.path)
    } catch let error as HelperError {
        throw error
    } catch let error as CocoaError where
        error.code == .fileNoSuchFile || error.code == .fileReadNoSuchFile
    {
        throw HelperError(code: "source_access_capability_missing")
    } catch {
        throw HelperError(code: "source_access_capability_invalid")
    }
    guard let size = (attributes[.size] as? NSNumber)?.intValue,
          size > 0, size <= maximumResponseBytes,
          attributes[.type] as? FileAttributeType == .typeRegular
    else {
        throw HelperError(code: "source_access_capability_missing")
    }
    do {
        let input = try FileHandle(forReadingFrom: path)
        defer { try? input.close() }
        let payload = input.readData(ofLength: maximumResponseBytes + 1)
        guard !payload.isEmpty, payload.count <= maximumResponseBytes else {
            throw HelperError(code: "source_access_capability_invalid")
        }
        let capability = try JSONDecoder().decode(StoredCapability.self, from: payload)
        guard (capability.sharedConfig == nil) == (capability.sharedConfigMeta == nil) else {
            throw HelperError(code: "source_access_capability_invalid")
        }
        return capability
    } catch {
        throw HelperError(code: "source_access_capability_invalid")
    }
}

private func synchronizeDirectory(_ directory: URL) throws {
    // FileHandle(forReadingFrom:) rejects directories on macOS. Use an owned
    // directory descriptor for the publication durability fence instead.
    let descriptor = Darwin.open(directory.path, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW)
    guard descriptor >= 0 else {
        throw HelperError(code: "source_access_capability_write_failed")
    }
    defer { _ = Darwin.close(descriptor) }
    guard Darwin.fsync(descriptor) == 0 else {
        throw HelperError(code: "source_access_capability_write_failed")
    }
}

private func writeCapability(_ capability: StoredCapability) throws {
    let directory = try containerDirectory()
    let target = directory.appendingPathComponent("capability.json", isDirectory: false)
    let temporary = directory.appendingPathComponent(
        ".capability.\(UUID().uuidString)", isDirectory: false
    )
    var publicationStarted = false
    do {
        let payload = try encodeLine(capability)
        guard FileManager.default.createFile(
            atPath: temporary.path, contents: nil, attributes: [.posixPermissions: 0o600]
        ) else {
            throw HelperError(code: "source_access_capability_write_failed")
        }
        let descriptor = try FileHandle(forWritingTo: temporary)
        try descriptor.write(contentsOf: payload)
        try descriptor.synchronize()
        try descriptor.close()
        try FileManager.default.setAttributes(
            [.posixPermissions: 0o600], ofItemAtPath: temporary.path
        )
        publicationStarted = true
        if FileManager.default.fileExists(atPath: target.path) {
            _ = try FileManager.default.replaceItemAt(target, withItemAt: temporary)
        } else {
            try FileManager.default.moveItem(at: temporary, to: target)
        }
        try FileManager.default.setAttributes(
            [.posixPermissions: 0o600], ofItemAtPath: target.path
        )
        try synchronizeDirectory(directory)
    } catch {
        try? FileManager.default.removeItem(at: temporary)
        throw HelperError(code: publicationStarted
            ? "source_access_capability_publication_uncertain"
            : "source_access_capability_write_failed")
    }
}

private func resolve(_ item: StoredItem, expectedKind: String) throws -> URL {
    guard !item.stale, item.identity.kind == expectedKind else {
        throw HelperError(code: "source_access_capability_stale")
    }
    guard let data = Data(base64Encoded: item.bookmark) else {
        throw HelperError(code: "source_access_capability_invalid")
    }
    var stale = false
    do {
        let url = try URL(
            resolvingBookmarkData: data,
            options: [.withSecurityScope, .withoutUI],
            relativeTo: nil,
            bookmarkDataIsStale: &stale
        )
        guard !stale, url.standardizedFileURL.path == item.identity.path else {
            throw HelperError(code: "source_access_capability_stale")
        }
        return url
    } catch let error as HelperError {
        throw error
    } catch {
        throw HelperError(code: "source_access_capability_stale")
    }
}

private func implicitBookmark(_ url: URL) throws -> String {
    let data = try url.bookmarkData(
        options: [],
        includingResourceValuesForKeys: nil,
        relativeTo: nil
    )
    guard !data.isEmpty, data.count <= 256 * 1024 else {
        throw HelperError(code: "source_access_bookmark_failed")
    }
    return data.base64EncodedString()
}

private func makeStoredItem(_ url: URL) throws -> StoredItem {
    let data = try url.bookmarkData(
        options: [.withSecurityScope, .securityScopeAllowOnlyReadAccess],
        includingResourceValuesForKeys: nil,
        relativeTo: nil
    )
    guard !data.isEmpty, data.count <= 256 * 1024 else {
        throw HelperError(code: "source_access_bookmark_failed")
    }
    let kind: String
    let values = try url.resourceValues(forKeys: [.isDirectoryKey, .isRegularFileKey])
    if values.isDirectory == true {
        kind = "account_directory"
    } else if values.isRegularFile == true {
        kind = url.lastPathComponent == "global_config.crc"
            ? "shared_config_meta_file" : "shared_config_file"
    } else {
        throw HelperError(code: "source_access_root_invalid")
    }
    let item = try identity(url, kind: kind, requireAccountLayout: false)
    return StoredItem(identity: item, bookmark: data.base64EncodedString(), stale: false)
}

private func picker(title: String, message: String, expected: ItemIdentity) throws -> URL {
    let application = NSApplication.shared
    application.setActivationPolicy(.accessory)
    let panel = NSOpenPanel()
    panel.title = title
    panel.message = message
    panel.prompt = "Authorize"
    panel.canChooseDirectories = expected.kind == "account_directory"
    panel.canChooseFiles = expected.kind != "account_directory"
    panel.allowsMultipleSelection = false
    panel.canCreateDirectories = false
    panel.showsHiddenFiles = true
    panel.resolvesAliases = true
    panel.directoryURL = URL(
        fileURLWithPath: expected.kind == "account_directory"
            ? expected.path : URL(fileURLWithPath: expected.path).deletingLastPathComponent().path,
        isDirectory: expected.kind == "account_directory"
    )
    guard panel.runModal() == .OK, let selected = panel.url else {
        throw HelperError(code: "source_access_picker_cancelled")
    }
    do {
        let actual = try identity(
            selected,
            kind: expected.kind,
            requireAccountLayout: expected.kind == "account_directory"
        )
        guard actual == expected else {
            throw HelperError(code: "source_access_root_mismatch")
        }
    } catch {
        selected.stopAccessingSecurityScopedResource()
        throw error
    }
    return selected
}

private func authorize(_ request: Request) throws -> Response {
    guard let expectedAccount = request.expectedAccount,
          let receiverProof = request.receiverProof
    else {
        throw HelperError(code: "source_access_request_invalid")
    }
    let accountURL = try picker(
        title: "Authorize Sightglass Account",
        message: "Select the configured WeChat account folder containing db_storage.",
        expected: expectedAccount
    )
    defer { accountURL.stopAccessingSecurityScopedResource() }
    let account = try makeStoredItem(accountURL)
    var shared: StoredItem?
    var sharedMeta: StoredItem?
    var sharedURL: URL?
    var sharedMetaURL: URL?
    defer {
        sharedMetaURL?.stopAccessingSecurityScopedResource()
        sharedURL?.stopAccessingSecurityScopedResource()
    }
    if request.includeSharedConfig,
       let expectedShared = request.expectedSharedConfig,
       let expectedMeta = request.expectedSharedConfigMeta
    {
        sharedURL = try picker(
            title: "Authorize Optional Sticker Configuration",
            message: "Select the exact all_users/config/global_config file.",
            expected: expectedShared
        )
        shared = try makeStoredItem(sharedURL!)
        sharedMetaURL = try picker(
            title: "Authorize Optional Sticker Metadata",
            message: "Select its exact all_users/config/global_config.crc file.",
            expected: expectedMeta
        )
        sharedMeta = try makeStoredItem(sharedMetaURL!)
    }
    let response = Response(
        schema: protocolSchema, ok: true, mode: "authorize", nonce: request.nonce,
        code: nil, helperVersion: helperVersion,
        helperDigest: request.expectedHelperDigest,
        capabilityPresent: true, grantsAccess: true,
        account: account.identity,
        accountDigest: identityDigest(account.identity),
        sharedConfig: shared?.identity,
        sharedConfigBookmark: try sharedURL.map { try implicitBookmark($0) },
        sharedConfigDigest: shared.map { identityDigest($0.identity) },
        sharedConfigMeta: sharedMeta?.identity,
        sharedConfigMetaBookmark: try sharedMetaURL.map { try implicitBookmark($0) },
        sharedConfigMetaDigest: sharedMeta.map { identityDigest($0.identity) },
        accountBookmark: try implicitBookmark(accountURL),
        receiverProofAccepted: true,
        receiverVerifier: receiverVerifier(receiverProof),
        entitlementReady: nil, containerReady: true
    )
    // Finish fallible exports/encoding before replacing any stored capability.
    _ = try encodeLine(response)
    try writeCapability(StoredCapability(
        schema: capabilitySchema,
        account: account,
        sharedConfig: shared,
        sharedConfigMeta: sharedMeta,
        receiverVerifier: receiverVerifier(receiverProof)
    ))
    return response
}

private func lease(_ request: Request) throws -> Response {
    guard let expectedAccount = request.expectedAccount,
          let receiverProof = request.receiverProof
    else {
        throw HelperError(code: "source_access_request_invalid")
    }
    let capability = try loadCapability()
    guard capability.schema == capabilitySchema else {
        throw HelperError(code: "source_access_capability_invalid")
    }
    guard proofMatches(receiverProof, verifier: capability.receiverVerifier) else {
        throw HelperError(code: "source_access_receiver_unauthorized")
    }
    guard capability.account.identity == expectedAccount,
          capability.account.stale == false
    else {
        throw HelperError(code: "source_access_account_binding_changed")
    }
    guard request.includeSharedConfig == (
        request.expectedSharedConfig != nil &&
        request.expectedSharedConfigMeta != nil
    )
    else {
        throw HelperError(code: "source_access_request_invalid")
    }
    if let expectedShared = request.expectedSharedConfig,
       let expectedMeta = request.expectedSharedConfigMeta
    {
        guard capability.sharedConfig?.identity == expectedShared,
              capability.sharedConfig?.stale == false,
              capability.sharedConfigMeta?.identity == expectedMeta,
              capability.sharedConfigMeta?.stale == false
        else {
            throw HelperError(code: "source_access_shared_config_binding_changed")
        }
    }
    let returnedShared = request.includeSharedConfig ? capability.sharedConfig : nil
    let returnedSharedMeta = request.includeSharedConfig ? capability.sharedConfigMeta : nil
    let accountURL = try resolve(capability.account, expectedKind: "account_directory")
    guard accountURL.standardizedFileURL.path == expectedAccount.path,
          accountURL.startAccessingSecurityScopedResource()
    else {
        throw HelperError(code: "source_access_capability_denied")
    }
    var sharedScope: URL?
    var sharedMetaScope: URL?
    defer {
        sharedMetaScope?.stopAccessingSecurityScopedResource()
        sharedScope?.stopAccessingSecurityScopedResource()
        accountURL.stopAccessingSecurityScopedResource()
    }
    do {
        if let item = returnedShared {
            let url = try resolve(item, expectedKind: "shared_config_file")
            guard url.startAccessingSecurityScopedResource() else {
                throw HelperError(code: "source_access_capability_denied")
            }
            sharedScope = url
        }
        if let item = returnedSharedMeta {
            let url = try resolve(item, expectedKind: "shared_config_meta_file")
            guard url.startAccessingSecurityScopedResource() else {
                throw HelperError(code: "source_access_capability_denied")
            }
            sharedMetaScope = url
        }
        let response = Response(
            schema: protocolSchema, ok: true, mode: "lease", nonce: request.nonce,
            code: nil, helperVersion: helperVersion,
            helperDigest: request.expectedHelperDigest,
            capabilityPresent: true, grantsAccess: true,
            account: expectedAccount,
            accountDigest: identityDigest(expectedAccount),
            sharedConfig: returnedShared?.identity,
            sharedConfigBookmark: try sharedScope.map { try implicitBookmark($0) },
            sharedConfigDigest: returnedShared.map { identityDigest($0.identity) },
            sharedConfigMeta: returnedSharedMeta?.identity,
            sharedConfigMetaBookmark: try sharedMetaScope.map {
                try implicitBookmark($0)
            },
            sharedConfigMetaDigest: returnedSharedMeta.map {
                identityDigest($0.identity)
            },
            accountBookmark: try implicitBookmark(accountURL),
            receiverProofAccepted: true,
            receiverVerifier: capability.receiverVerifier,
            entitlementReady: nil, containerReady: true
        )
        try emit(response)
        // Keep every helper app-scope URL held until the parent explicitly ends
        // the bounded exchange by closing stdin.
        while FileHandle.standardInput.readData(ofLength: 1).count == 1 {}
        return response
    } catch {
        throw error
    }
}

private func check(_ request: Request) throws -> Response {
    _ = try containerDirectory()
    guard Bundle.main.bundleURL.pathExtension.lowercased() == "app",
          Bundle.main.executableURL != nil
    else {
        throw HelperError(code: "source_access_helper_bundle_invalid")
    }
    let path = try capabilityPath()
    return Response(
        schema: protocolSchema, ok: true, mode: "check", nonce: request.nonce,
        code: nil, helperVersion: helperVersion,
        helperDigest: request.expectedHelperDigest,
        capabilityPresent: FileManager.default.fileExists(atPath: path.path),
        grantsAccess: false, account: nil, accountDigest: nil,
        sharedConfig: nil, sharedConfigBookmark: nil,
        sharedConfigDigest: nil, sharedConfigMeta: nil,
        sharedConfigMetaBookmark: nil, sharedConfigMetaDigest: nil,
        accountBookmark: nil,
        receiverProofAccepted: false,
        receiverVerifier: nil,
        entitlementReady: true, containerReady: true
    )
}

@main
private struct Main {
    static func main() {
        var acceptedRequest: Request?
        do {
            let request = try readRequest()
            acceptedRequest = request
            let response: Response
            switch request.mode {
            case "check":
                response = try check(request)
            case "authorize":
                response = try authorize(request)
            default:
                response = try lease(request)
            }
            if request.mode != "lease" || !response.ok {
                try emit(response)
            }
        } catch let error as HelperError {
            try? emit(errorResponse(error.code, request: acceptedRequest))
            exit(1)
        } catch {
            try? emit(errorResponse("source_access_helper_failed", request: acceptedRequest))
            exit(1)
        }
    }
}

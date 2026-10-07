import Foundation
import NaturalLanguage

// Local installed sentence assets only. This helper never requests a download.
struct Request: Decodable { let language: String; let texts: [String] }
struct Response: Encodable { let schema: String; let model: String; let revision: Int; let dimensions: Int; let vectors: [[Double]] }
do {
    let input = FileHandle.standardInput.readDataToEndOfFile()
    let request = try JSONDecoder().decode(Request.self, from: input)
    guard request.texts.count <= 128,
          request.texts.allSatisfy({ $0.count <= 16000 }),
          ["en", "zh-Hans"].contains(request.language),
          let embedding = NLEmbedding.sentenceEmbedding(for: NLLanguage(rawValue: request.language))
    else { throw NSError(domain: "sightglass.local.encoder.unavailable", code: 1) }
    var vectors: [[Double]] = []
    for text in request.texts {
        guard let vector = embedding.vector(for: text), vector.count == embedding.dimension,
              vector.allSatisfy({ $0.isFinite }) else {
            throw NSError(domain: "sightglass.local.encoder.failed", code: 2)
        }
        vectors.append(vector)
    }
    let output = Response(schema: "sightglass.encoder-batch.v1", model: "apple.nlembedding.sentence.\(request.language)", revision: embedding.revision, dimensions: embedding.dimension, vectors: vectors)
    FileHandle.standardOutput.write(try JSONEncoder().encode(output))
} catch {
    // No input, model paths or error description in diagnostics.
    FileHandle.standardError.write(Data("sightglass encoder failed\n".utf8))
    exit(1)
}

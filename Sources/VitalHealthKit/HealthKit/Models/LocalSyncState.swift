import Foundation
import VitalCore

internal struct LocalSyncState: Codable {
  let historicalStageAnchor: Date
  let defaultDaysToBackfill: Int

  let ingestionEnd: Date?
  let perDeviceActivityTS: Bool

  let expiresAt: Date

  func historicalStartDate(for resource: VitalResource) -> Date {
    Date.dateAgo(historicalStageAnchor, days: defaultDaysToBackfill)
  }
}


struct SyncInstruction: CustomStringConvertible {
  let stage: Stage
  let query: Range<Date>

  public var description: String {
    return "\(stage): \(query.lowerBound) - \(query.upperBound)"
  }

  var taggedPayloadStage: TaggedPayload.Stage {
    switch stage {
    case .daily:
      return .daily
    case .historical:
      return .historical(start: query.lowerBound, end: query.upperBound)
    }
  }
}

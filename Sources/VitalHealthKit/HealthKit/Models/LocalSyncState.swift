import Foundation
import VitalCore

internal struct LocalSyncState: Codable {
  let historicalStageAnchor: Date
  let defaultDaysToBackfill: Int
  let teamDataPullPreferences: TeamDataPullPreferences?

  let ingestionEnd: Date?
  let perDeviceActivityTS: Bool

  let expiresAt: Date

  func historicalStartDate(for resource: VitalResource) -> Date {
    let backfillType = resource.resourceToBackfillType();
    let daysToBackfill = teamDataPullPreferences?.backfillTypeOverrides?[backfillType]?.historicalDaysToPull ?? teamDataPullPreferences?.historicalDaysToPull;
    return Date.dateAgo(historicalStageAnchor, days: daysToBackfill ?? defaultDaysToBackfill)
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

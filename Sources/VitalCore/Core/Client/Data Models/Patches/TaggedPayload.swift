import Foundation

public struct TaggedPayload: Encodable {
  public let stage: String
  public let startDate: Date?
  public let endDate: Date?
  public let timeZone: String
  public let provider: UserConnection.Slug
  public let data: VitalAnyEncodable
  
  public init(
    stage: Stage = .daily,
    timeZone: TimeZone,
    provider: UserConnection.Slug = .manual,
    data: VitalAnyEncodable
  ) {
    self.provider = provider
    self.data = data
    self.timeZone = timeZone.identifier
    
    switch stage {
      case .daily:
        self.stage = "daily"
        self.startDate = nil
        self.endDate = nil
        
      case let .historical(start: startDate, end: endDate):
        self.stage = "historical"
        self.startDate = startDate
        self.endDate = endDate
    }
  }
}

public extension TaggedPayload {
  enum Stage: CustomStringConvertible {
    case daily
    case historical(start: Date, end: Date)
    
    public var isDaily: Bool {
      switch self {
        case .daily:
          return true
        case .historical:
          return false
      }
    }
    
    public var description: String {
      switch self {
        case .daily:
          return "daily"
        case let .historical(start: startDate, end: endDate):
          return "historical: \(startDate) - \(endDate)"
      }
    }
  }
  
  enum Data: Encodable {
    public enum Vitals: Encodable {
      case glucose([QuantitySample])
    }
    
    case profile(ProfilePatch)
    case activity(ActivityPatch)
    case workout(WorkoutPatch)
    case sleep(SleepPatch)
    case body(BodyPatch)
    case vitals(Vitals)
  }
}


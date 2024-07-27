@_spi(VitalSDKInternals) import VitalCore
import Foundation

public struct SyncProgress: Codable {
  public var resources: [VitalResource: Resource] = [:]

  public init() {}

  public enum SystemEventType: Int, Codable {
    case receivedNotification = 0
  }

  public struct Event<EventType: Equatable & Codable>: Codable, Identifiable {
    public let timestamp: Date
    public let type: EventType

    public var id: Date { timestamp }
  }

  public enum SyncStatus: Int, Codable {
    case deprioritized = 0
    case started = 1
    case readChunk = 2
    case uploadedChunk = 3
    case noData = 6
    case timeout = 4
    case completed = 5

    public var isInProgress: Bool {
      switch self {
      case .deprioritized, .started, .readChunk, .uploadedChunk:
        return true
      case .completed, .noData, .timeout:
        return false
      }
    }
  }

  public struct Sync: Codable, Identifiable {
    public let start: Date
    public var end: Date?
    public private(set) var statuses: [Event<SyncStatus>]

    public var lastStatus: SyncStatus {
      statuses.last!.type
    }

    public var id: Date { start }

    public init(start: Date, status: SyncStatus) {
      self.start = start
      self.statuses = [Event(timestamp: start, type: status)]
    }

    public mutating func append(_ status: SyncStatus, at timestamp: Date = Date()) {
      statuses.append(Event(timestamp: timestamp, type: status))
    }
  }

  public struct SyncID {
    public let rawValue: Date

    public init() {
      rawValue = Date()
    }
  }

  public struct Resource: Codable {
    public var syncs: [Sync] = []
    public var systemEvents: [Event<SystemEventType>] = []
    public var uploadedChunks: Int = 0
    public var firstAsked: Date? = nil

    public var latestSync: Sync? {
      syncs.last
    }

    mutating func with(_ action: (inout Self) -> Void) {
      action(&self)
    }
  }
}

final class SyncProgressStore {
  private var state: SyncProgress {
    didSet {
      try? VitalGistStorage.shared.set(state, for: SyncProgressGistKey.self)
    }
  }
  private let lock = NSLock()

  static let shared = SyncProgressStore()

  init() {
    state = VitalGistStorage.shared.get(SyncProgressGistKey.self) ?? SyncProgress()
  }

  func get() -> SyncProgress {
    lock.withLock { state }
  }

  func clear() {
    lock.withLock {
      state = SyncProgress()
      try? VitalGistStorage.shared.set(Optional<SyncProgress>.none, for: SyncProgressGistKey.self)
    }
  }

  func recordSync(_ resource: RemappedVitalResource, _ status: SyncProgress.SyncStatus, for id: SyncProgress.SyncID) {
    mutate(CollectionOfOne(resource)) {
      let now = Date()

      let latestSync = $0.syncs.last
      let appendsToLatestSync = (
        // Shares the same start timestamp
        latestSync?.start == id.rawValue

        // OR last status is deprioritized
        || status == .deprioritized && latestSync?.lastStatus == .deprioritized
      )

      if appendsToLatestSync {
        let index = $0.syncs.count - 1
        $0.syncs[index].append(status, at: now)

        switch status {
        case .completed, .timeout, .noData:
          $0.syncs[index].end = now

        default:
          break
        }

      } else {
        if $0.syncs.count > 50 {
          $0.syncs.removeFirst()
        }

        $0.syncs.append(
          SyncProgress.Sync(start: id.rawValue, status: status)
        )
      }
    }
  }

  func recordSystem(_ resources: some Sequence<RemappedVitalResource>, _ eventType: SyncProgress.SystemEventType) {
    mutate(resources) {
      let now = Date()

      // Capture this new event if the event type is different or 2 seconds have elapsed.
      let shouldCapture = $0.systemEvents.first.map { $0.type != eventType || now.timeIntervalSince($0.timestamp) >= 2.0 } ?? true
      guard shouldCapture else { return }

      if $0.systemEvents.count > 25 {
        $0.systemEvents.removeFirst()
      }

      $0.systemEvents.append(
        SyncProgress.Event(timestamp: now, type: eventType)
      )
    }
  }

  public func mutate(_ resources: some Sequence<RemappedVitalResource>, action: (inout SyncProgress.Resource) -> Void) {
    lock.withLock {
      for resource in resources {
        state.resources[resource.wrapped, default: SyncProgress.Resource()]
          .with(action)
      }
    }
  }
}

enum SyncProgressGistKey: GistKey {
  typealias T = SyncProgress

  static var identifier: String = "vital_healthkit_progress"
}
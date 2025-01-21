import HealthKit
import Combine
import os.log
import UIKit
@_spi(VitalSDKInternals) import VitalCore
import BackgroundTasks

let processingTaskIdentifier = "io.tryvital.VitalHealthKit.ProcessingTask"

public enum PermissionOutcome: Equatable {
  case success
  case failure(String)
  case healthKitNotAvailable
}

@objc public class VitalHealthKitClient: NSObject {
  public enum Status {
    case failedSyncing(VitalResource, Error?)
    case successSyncing(VitalResource, ProcessedResourceData)
    case nothingToSync(VitalResource)
    case syncing(VitalResource)
    case syncingCompleted
  }
  
  public static var shared: VitalHealthKitClient {
    clientInitLock.withLock {
      guard let value = client else {
        let newClient = VitalHealthKitClient()
        Self.client = newClient
        Self.bind(newClient, core: VitalClient.shared)
        return newClient
      }

      return value
    }
  }

  private static let clientInitLock = NSLock()
  private static var client: VitalHealthKitClient?

  let storage: VitalHealthKitStorage

  private let store: VitalHealthKitStore
  private let secureStorage: VitalSecureStorage
  private let vitalClient: VitalClientProtocol
  
  private let _status: PassthroughSubject<Status, Never>
  private var backgroundDeliveryTask: BackgroundDeliveryTask? = nil

  private var cancellables: Set<AnyCancellable> = []

  internal let backgroundDeliveryEnabled: ProtectedBox<Bool> = .init(value: false)
  private let backendSyncStateParkingLot = ParkingLot()

  let configuration: ProtectedBox<Configuration>

  let syncSerializerLock = NSLock()
  var syncSerializer: [RemappedVitalResource: ParkingLot] = [:]

  private static var syncAlongsideTasks: [VitalResource: (VitalResource) async -> Void] = [:]

  private var scope = SupervisorScope()

  private var isAutoSyncConfigured: Bool {
    backgroundDeliveryEnabled.value ?? false
  }
  
  public var status: AnyPublisher<Status, Never> {
    return _status.eraseToAnyPublisher()
  }

  init(
    configuration: ProtectedBox<Configuration> = .init(),
    store: VitalHealthKitStore = .live,
    storage: VitalHealthKitStorage = .init(storage: .live),
    secureStorage: VitalSecureStorage = .init(keychain: .live),
    vitalClient: VitalClientProtocol = .live
  ) {
    self.store = store
    self.storage = storage
    self.secureStorage = secureStorage
    self.vitalClient = vitalClient
    self.configuration = configuration
    
    self._status = PassthroughSubject<Status, Never>()
    
    super.init()
  }

  private static func bind(_ client: VitalHealthKitClient, core: VitalClient) {
    core.registerSignoutTask {
      await client.resetAutoSync()
    }

    client.registerProcessingTaskHandlers()

    AppStateTracker.shared.register { state in
      switch state.status {
      case .background, .launching:
        break

      case .foreground:
        client.scheduleDeprioritizedResourceRetries()

        // Sync profile since most of the content is not observable.
        if client.hasAskedForPermission(resource: .profile) {
          Task(priority: .high) {
            await client.sync(RemappedVitalResource(wrapped: .profile), [.maintenanceTask])
          }
        }

      case .terminating:
        Task(priority: .high) {
          await client.scope.cancel()
          SyncProgressStore.shared.flush()
        }
      }
    }
  }
  
  /// Only use this method if you are working from Objc.
  /// Please use the async/await configure method when working from Swift.
  @objc public static func configure(
    backgroundDeliveryEnabled: Bool = false,
    numberOfDaysToBackFill: Int = 90,
    logsEnabled: Bool = true
  ) {
    configure(
      .init(
        backgroundDeliveryEnabled: backgroundDeliveryEnabled,
        numberOfDaysToBackFill: numberOfDaysToBackFill,
        logsEnabled: logsEnabled,
        mode: .automatic
      )
    )
  }

  // IMPORTANT: The synchronous `configure(3)` is the preferred version over this async one.
  //
  // The async overload is still kept here for source compatibility, because Swift always ignores
  // the non-async overload sharing the same method signature, even if the async version is
  // deprecated.
  @_disfavoredOverload
  public static func configure(
    _ configuration: Configuration = .init()
  ) async {
    self.shared.setConfiguration(configuration: configuration)
  }
  
  public static func configure(
    _ configuration: Configuration = .init()
  ) {
    self.shared.setConfiguration(configuration: configuration)
  }

  @objc(automaticConfigurationWithCompletion:)
  public static func automaticConfiguration(completion: (() -> Void)? = nil) {
    do {
      let secureStorage = self.shared.secureStorage
      guard let payload: Configuration = try secureStorage.get(key: health_secureStorageKey) else {
        completion?()
        return
      }
      
      configure(payload)
      VitalClient.automaticConfiguration(completion: completion)
    } catch let error {
      completion?()
      /// Bailout, there's nothing else to do here.
      /// (But still try to log it if we have a logger around)
      VitalLogger.healthKit.error("Failed to perform automatic configuration: \(error)")
    }
  }

  /// **Synchronously** set the configuration and kick off the side effects.
  ///
  /// - important: This cannot not be `async` due to background observer registration
  /// timing requirement by HealthKit. Instead, spawn async tasks if necessary,
  func setConfiguration(
    configuration: Configuration
  ) {
    do {
      try secureStorage.set(value: configuration, key: health_secureStorageKey)
    }
    catch {
      VitalLogger.healthKit.info("We weren't able to securely store Configuration: \(error)")
    }
    
    self.configuration.set(value: configuration)
    
    if backgroundDeliveryEnabled.value != true {
      backgroundDeliveryEnabled.set(value: true)

      checkBackgroundUpdates(isBackgroundEnabled: configuration.backgroundDeliveryEnabled)
      scheduleUnnotifiedResourceRescue()
    }
  }
}

public extension VitalHealthKitClient {
  struct Configuration: Codable {
    public enum DataPushMode: String, Codable {
      case manual
      case automatic
      
      var isManual: Bool {
        switch self {
          case .manual:
            return true
          case .automatic:
            return false
        }
      }
      
      var isAutomatic: Bool {
        return isManual == false
      }
    }
    
    public let backgroundDeliveryEnabled: Bool
    public let numberOfDaysToBackFill: Int
    public let logsEnabled: Bool
    public let mode: DataPushMode
    
    public init(
      backgroundDeliveryEnabled: Bool = false,
      numberOfDaysToBackFill: Int = 90,
      logsEnabled: Bool = true,
      mode: DataPushMode = .automatic
    ) {
      self.backgroundDeliveryEnabled = backgroundDeliveryEnabled
      self.numberOfDaysToBackFill = min(numberOfDaysToBackFill, 365)
      self.logsEnabled = logsEnabled
      self.mode = mode
    }
  }

  private struct BackgroundDeliveryTask {
    let task: TaskHandle?
    let resources: Set<RemappedVitalResource>
    let objectTypes: Set<HKObjectType>
    let streamContinuation: AsyncStream<BackgroundDeliveryStage>.Continuation

    func cancel() {
      streamContinuation.finish()
      task?.cancel()
    }
  }
}

extension VitalHealthKitClient {
  
  private func checkBackgroundUpdates(isBackgroundEnabled: Bool) {
    guard isBackgroundEnabled else { return }

    let state = authorizationState(store: self.store)
    let currentTask = self.backgroundDeliveryTask

    // Reconfigure the task only if the set of resources has changed, or we have not configured it
    // before.
    guard 
      state.activeResources != currentTask?.resources
        || state.determinedObjectTypes != currentTask?.objectTypes
    else { return }

    /// If it's already running, cancel it
    currentTask?.cancel()

    let bundles: Set<[HKSampleType]> = Set(
      observedSampleTypes()
        .map { Set($0).intersection(state.determinedObjectTypes).compactMap { $0 as? HKSampleType } }
        .filter { !$0.isEmpty }
    )

    if bundles.isEmpty {
      VitalLogger.healthKit.info("Not observing any type")
    }

    /// Enable background deliveries
    enableBackgroundDelivery(for: bundles.lazy.flatMap { $0 })

    /// Submit BGProcessingTasks
    scope.task { await self.submitProcessingTasks() }

    let stream: AsyncStream<BackgroundDeliveryStage>
    let streamContinuation: AsyncStream<BackgroundDeliveryStage>.Continuation

    if #available(iOS 15.0, *) {
      (stream, streamContinuation) = bundledBackgroundObservers(for: bundles)
    } else {
      (stream, streamContinuation) = backgroundObservers(for: bundles.lazy.flatMap { $0 })
    }

    let payloadListener = scope.task(priority: .userInitiated) {
      var bufferedPayloads: [BackgroundDeliveryPayload] = []
      var timer: Task<Void, Never>?

      defer {
        // If there is any accidential leftover payload, make the completion callback anyway.
        bufferedPayloads.forEach { $0.completion(.completed) }
        timer?.cancel()
      }

      for await stage in stream {
        switch stage {
        case let .received(payload):
          if Task.isCancelled {
            payload.completion(.completed)
            continue
          }

          bufferedPayloads.append(payload)
          VitalLogger.healthKit.info("buffered: \(payload)", source: "BgDelivery")

          if timer == nil {
            timer = Task(priority: .high) {
              // Throttle by 16ms
              // Catch as many parallel HealthKit observer callouts as possible.
              try? await Task.sleep(nanoseconds: NSEC_PER_MSEC * 16)
              streamContinuation.yield(.evaluate)
            }
          }

        case .evaluate:
          timer = nil
          let payloads = bufferedPayloads
          bufferedPayloads = []

          // Task is not cancelled — we must call the HealthKit completion handler irrespective of
          // the sync process outcome. This is to avoid triggering the "strike on 3rd missed delivery"
          // rule of HealthKit background delivery.
          //
          // Since we have fairly frequent delivery anyway, each of which will implicit retry from
          // where the last sync has left off, this unconfigurable exponential backoff retry
          // behaviour adds little to no value in maintaining data freshness.
          //
          // (except for the task cancellation redelivery expectation stated above).
          defer { payloads.forEach { $0.completion(.completed) } }

          let prioritizedResources = Set(payloads.flatMap(\.resources))
            .sorted(by: { $0.wrapped.priority < $1.wrapped.priority })
          let syncsConcurrently = AppStateTracker.shared.state.status == .foreground

          VitalLogger.healthKit.info(
            "dequeued: \(prioritizedResources); will sync \(syncsConcurrently ? "concurrent" : "serially")",
            source: "BgDelivery"
          )

          if syncsConcurrently {
            await withTaskGroup(of: Void.self) { group in
              for resource in prioritizedResources {
                group.addTask {
                  await self.sync(resource, [.healthKit])
                }
              }
            }
          } else {
            for resource in prioritizedResources {
              await self.sync(resource, [.healthKit])
            }
          }
        }
      }
    }

    self.backgroundDeliveryTask = BackgroundDeliveryTask(
      task: payloadListener,
      resources: state.activeResources,
      objectTypes: state.determinedObjectTypes,
      streamContinuation: streamContinuation
    )
  }
  
  private func enableBackgroundDelivery(for sampleTypes: some Sequence<HKSampleType>) {
    for sampleType in sampleTypes {
      store.enableBackgroundDelivery(sampleType, .immediate) { success, failure in
        
        guard failure == nil && success else {
          VitalLogger.healthKit.error("failed: \(sampleType.shortenedIdentifier); error = \(String(describing: failure))", source: "EnableBgDelivery")
          return
        }
        
        VitalLogger.healthKit.info("enabled: \(sampleType.shortenedIdentifier)", source: "EnableBgDelivery")
      }
    }
  }

  // We can only register handlers once, and this must happen before the app finishes launching.
  func registerProcessingTaskHandlers() {
    let scheduler = BGTaskScheduler.shared
    let declaredBgTasks = Set(Bundle.main.object(forInfoDictionaryKey: "BGTaskSchedulerPermittedIdentifiers") as? [String] ?? [])

    if declaredBgTasks.contains(processingTaskIdentifier) {
      scheduler.register(forTaskWithIdentifier: processingTaskIdentifier, using: nil) { task in

        VitalLogger.healthKit.info("begin", source: "ProcessingTask")
        defer { VitalLogger.healthKit.info("ended", source: "ProcessingTask") }

        task.expirationHandler = {
          VitalLogger.healthKit.info("expired", source: "ProcessingTask")
          SyncProgressStore.shared.flush()
          task.setTaskCompleted(success: false)
        }

        self.scope.task(priority: .userInitiated) {
          defer {
            task.setTaskCompleted(success: true)
          }

          guard VitalClient.status.contains(.signedIn) else {
            VitalLogger.healthKit.info("not signed in", source: "ProcessingTask")
            return
          }

          let state = authorizationState(store: self.store)

          SyncProgressStore.shared.recordSystem(state.activeResources, .backgroundProcessingTask)

          await withTaskGroup(of: Void.self) { group in
            for resource in state.activeResources {
              group.addTask {
                await self.sync(resource, [.processingTask])
              }
            }
          }

          await self.submitProcessingTasks()
        }
      }
    }
  }

  func submitProcessingTasks() async {
    let scheduler = BGTaskScheduler.shared
    let declaredBgTasks = Set(Bundle.main.object(forInfoDictionaryKey: "BGTaskSchedulerPermittedIdentifiers") as? [String] ?? [])

    if declaredBgTasks.contains(processingTaskIdentifier) {
      let requests = await scheduler.pendingTaskRequests()
      if requests.contains(where: { $0.identifier == processingTaskIdentifier }) {
        VitalLogger.healthKit.info("found existing", source: "ProcessingTask")
        return
      }

      let request: BGProcessingTaskRequest
      
      if #available(iOS 17.0, *) {
        request = BGHealthResearchTaskRequest(identifier: processingTaskIdentifier)
      } else {
        request = BGProcessingTaskRequest(identifier: processingTaskIdentifier)
      }

      request.requiresExternalPower = true
      request.requiresNetworkConnectivity = true

      // Processing Tasks should be at minimum 6 hours apart
      request.earliestBeginDate = Date().addingTimeInterval(3600 * 6)

      do {
        try scheduler.submit(request)
        VitalLogger.healthKit.info("submitted", source: "ProcessingTask")

      } catch let error {
        VitalLogger.healthKit.info("submission failed: \(error)", source: "ProcessingTask")
      }
    }
  }

  /// HKObserverQuery does not callout at app launch on resources that have no data.
  /// Rescue these resources by checking t
  func scheduleUnnotifiedResourceRescue() {
    scope.task { @MainActor in
      // 1 seconds after app launch or Ask
      try await Task.sleep(nanoseconds: 1 * NSEC_PER_SEC)

      let state = authorizationState(store: self.store)

      let progress = SyncProgressStore.shared.get()
      let unnotifiedResources = state.activeResources.filter { resource in
        guard let resourceProgress = progress.backfillTypes[resource.wrapped.backfillType] else {
          // Doesn't even show up in `SyncProgress.resources`
          return true
        }
        // OR has no sync attempt
        // OR has errored
        let lastStatus = resourceProgress.latestSync?.lastStatus

        return lastStatus == nil || lastStatus == .error || lastStatus == .cancelled
        || lastStatus == .timedOut || lastStatus == .started || lastStatus == .deprioritized
      }

      // Rescue these resources
      await withTaskGroup(of: Void.self) { group in
        for resource in unnotifiedResources {
          await self.sync(resource, [.maintenanceTask])
        }
      }
    }
  }

  @available(iOS 15.0, *)
  private func bundledBackgroundObservers(
    for typesBundle: Set<[HKSampleType]>
  ) -> (AsyncStream<BackgroundDeliveryStage>, AsyncStream<BackgroundDeliveryStage>.Continuation) {

    var _continuation: AsyncStream<BackgroundDeliveryStage>.Continuation!

    let stream = AsyncStream<BackgroundDeliveryStage> { continuation in
      _continuation = continuation

      var queries: [HKObserverQuery] = []

      for typesToObserve in typesBundle {

        let descriptors = typesToObserve.map {
          HKQueryDescriptor(sampleType: $0, predicate: nil)
        }

        let query = HKObserverQuery(queryDescriptors: descriptors) { query, sampleTypes, handler, error in

          guard error == nil else {
            VitalLogger.healthKit.error("observer errored for \(typesToObserve.map(\.shortenedIdentifier)); error = \(String(describing: error)).", source: "HealthKit")
            return
          }

          // Check this after checking error, because sampleTypes is usually nil when error is
          // populated.
          guard let sampleTypes = sampleTypes else {
            VitalLogger.healthKit.error("unexpected callout with no sample type", source: "HealthKit")
            return
          }

          // It appears that the iOS 15+ HKObserverQuery might pass us `HKSampleType`s that is
          // outside the conditions we specified via `descriptors`. Filter out any unsolicited types
          // before proceeding.
          let filteredSampleTypes = sampleTypes.intersection(typesToObserve)

          if filteredSampleTypes.isEmpty {
            handler()
          } else {

            let matches = Set(filteredSampleTypes.flatMap(VitalHealthKitStore.sampleTypeToVitalResource(type:)))
            let remapped = Set(matches.map(VitalHealthKitStore.remapResource))
              .sorted(by: { $0.wrapped.priority < $1.wrapped.priority })

            let payload = BackgroundDeliveryPayload(
              resources: remapped,
              completion: { completion in
                if completion == .completed {
                  handler()
                }
              }
            )
            VitalLogger.healthKit.info("notified: \(payload)", source: "HealthKit")

            continuation.yield(.received(payload))

            SyncProgressStore.shared.recordSystem(
              remapped,
              healthkitCalloutEventType(AppStateTracker.shared.state.status)
            )
          }
        }

        queries.append(query)
        store.execute(query)
      }

      /// If the task is cancelled, make sure we clean up the existing queries
      continuation.onTermination = { @Sendable [queries] _ in
        queries.forEach { query in
          self.store.stop(query)
        }
      }
    }

    return (stream, _continuation)
  }
  
  private func backgroundObservers(
    for sampleTypes: some Sequence<HKSampleType>
  ) -> (AsyncStream<BackgroundDeliveryStage>, AsyncStream<BackgroundDeliveryStage>.Continuation) {
    var _continuation: AsyncStream<BackgroundDeliveryStage>.Continuation!

    let stream = AsyncStream<BackgroundDeliveryStage> { continuation in
      _continuation = continuation

      var queries: [HKObserverQuery] = []
      
      for sampleType in sampleTypes {
        let query = HKObserverQuery(sampleType: sampleType, predicate: nil) { query, handler, error in

          guard error == nil else {
            VitalLogger.healthKit.error("observer errored for \(sampleType.shortenedIdentifier); error = \(String(describing: error)).", source: "HealthKit")
            return
          }

          let matches = Set(VitalHealthKitStore.sampleTypeToVitalResource(type: sampleType))
          let remapped = Set(matches.map(VitalHealthKitStore.remapResource))
            .sorted(by: { $0.wrapped.priority < $1.wrapped.priority })

          VitalLogger.healthKit.info("notified: \(remapped.map(\.wrapped.logDescription).joined(separator: ","))", source: "HealthKit")

          let payload = BackgroundDeliveryPayload(
            resources: remapped,
            completion: { completion in
              if completion == .completed {
                handler()
              }
            }
          )
          VitalLogger.healthKit.info("notified: \(payload)", source: "HealthKit")

          continuation.yield(.received(payload))

          SyncProgressStore.shared.recordSystem(
            remapped,
            healthkitCalloutEventType(AppStateTracker.shared.state.status)
          )
        }

        queries.append(query)
        store.execute(query)
      }
      
      /// If the task is cancelled, make sure we clean up the existing queries
      continuation.onTermination = { @Sendable [queries] _ in
        queries.forEach { query in
          self.store.stop(query)
        }
      }
    }

    return (stream, _continuation)
  }
  
  public func syncData() {
    let state = authorizationState(store: self.store)
    syncData(for: state.activeResources.map(\.wrapped))
  }

  public func syncData(for resources: [VitalResource]) {
    let remappedResources = Set(resources.map(VitalHealthKitStore.remapResource(_:)))

    scope.task(priority: .high) { @MainActor in
      await withTaskGroup(of: Void.self) { group in
        for resource in remappedResources {
          group.addTask { await self.sync(resource, [.manual]) }
        }
      }
    }
  }

  private func resetAutoSync() async {
    VitalLogger.healthKit.info("begin", source: "Reset")

    await scope.cancel()

    VitalLogger.healthKit.info("cancelled outstanding tasks", source: "Reset")

    await store.disableBackgroundDelivery()

    // Install a new clean scope.
    scope = SupervisorScope()

    backgroundDeliveryTask = nil
    backgroundDeliveryEnabled.set(value: false)

    SyncProgressStore.shared.clear()

    VitalLogger.healthKit.info("done", source: "Reset")
  }

  private func getLocalSyncState(syncID: SyncProgress.SyncID) async throws -> LocalSyncState {
    // If we have a LocalSyncState with valid TTL, return it.
    if 
      let state = storage.getLocalSyncState(),
      state.expiresAt > Date()
    {
      return state
    }

    guard backendSyncStateParkingLot.tryTo(.enable) else {
      try await backendSyncStateParkingLot.parkIfNeeded()

      // Try again
      return try await getLocalSyncState(syncID: syncID)
    }

    defer { _ = backendSyncStateParkingLot.tryTo(.disable) }

    // Double check if a LocalSyncState could have already been computed concurrently
    // between getLocalSyncState() and tryTo(.enable).
    if
      let state = storage.getLocalSyncState(),
      state.expiresAt > Date()
    {
      return state
    }

    VitalLogger.healthKit.info("revalidating", source: "LocalSyncState")
    SyncProgressStore.shared.recordSync(syncID, .revalidatingSyncState)

    let previousState = storage.getLocalSyncState()
    let configuration = await configuration.get()

    /// Make sure the user has a connected source set up
    try await vitalClient.checkConnectedSource(.appleHealthKit)

    let now = Date()
    let proposedStart = Date.dateAgo(now, days: configuration.numberOfDaysToBackFill)

    let backendState = try await vitalClient.sdkStateSync(
      UserSDKSyncStateBody(
        tzinfo: TimeZone.current.identifier,
        requestStartDate: proposedStart,
        requestEndDate: now
      )
    )

    if backendState.status != .active {
      VitalLogger.healthKit.info("connection is paused", source: "LocalSyncState")
      throw VitalHealthKitClientError.connectionPaused
    }

    let state = LocalSyncState(
      historicalStageAnchor: previousState?.historicalStageAnchor ?? now,
      defaultDaysToBackfill: previousState?.defaultDaysToBackfill ?? configuration.numberOfDaysToBackFill,
      teamDataPullPreferences: previousState?.teamDataPullPreferences ?? backendState.pullPreferences,

      ingestionStart: backendState.ingestionStart ?? previousState?.ingestionStart ?? .distantPast,
      // The query upper bound (end date for historical & daily) is normally open-ended.
      // In other words, `ingestionEnd` is typically nil.
      //
      // The only exception is if an ingestion end was set, in which case the most up-to-date
      // ingestion end date dictates the query upper bound.
      ingestionEnd: backendState.requestEndDate,

      perDeviceActivityTS: backendState.perDeviceActivityTs ?? false,

      // When we should revalidate the LocalSyncState again.
      expiresAt: Date().addingTimeInterval(Double(backendState.expiresIn ?? 14400)),

      reportingInterval: backendState.reportingInterval ?? previousState?.reportingInterval
    )

    try storage.setLocalSyncState(state)

    VitalLogger.healthKit.info("updated; \(state)", source: "LocalSyncState")

    return state
  }

  private func computeSyncInstruction(
    _ resource: RemappedVitalResource,
    syncID: SyncProgress.SyncID
  ) async throws -> (SyncInstruction, LocalSyncState) {

    let state = try await getLocalSyncState(syncID: syncID)

    let hasCompletedHistoricalStage = storage.historicalStageDone(for: resource)
    || resource.wrapped == .profile

    let now = Date()
    let query = state.historicalStartDate(for: resource.wrapped) ..< (state.ingestionEnd ?? now)

    let instruction = SyncInstruction(stage: hasCompletedHistoricalStage ? .daily : .historical, query: query)

    if !hasCompletedHistoricalStage {
      // Report historical stage range
      try await vitalClient.sdkStartHistoricalStage(
        UserSDKHistoricalStageBeginBody(
          rangeStart: query.lowerBound,
          rangeEnd: query.upperBound,
          backfillType: resource.wrapped.backfillType
        )
      )
    }

    return (instruction, state)
  }

  private func prioritizeSync(_ remappedResource: RemappedVitalResource, _ tags: Set<SyncContextTag>) -> Bool {
    if storage.historicalStageDone(for: remappedResource) {
      // Prioritization only affects historical stage.
      // If the resource is done with historical stage, it should not be subject to prioritization.
      return true
    }

    let priority = remappedResource.wrapped.priority
    let prerequisites = VitalResource.all.filter { $0.priority < priority }

    let state = authorizationState(store: self.store)
    let resourcesToCheck = state.activeResources
      .intersection(prerequisites.map(VitalHealthKitStore.remapResource))

    // Deprioritized until all prioritized resources have finished their historical stage.
    // If resourcesToCheck is empty, this returns true, i.e., no waiting.
    return resourcesToCheck.allSatisfy(storage.historicalStageDone(for:))
  }

  private func sync(_ remappedResource: RemappedVitalResource, _ tags: Set<SyncContextTag>) async {
    let progressStore = SyncProgressStore.shared
    let progressReporter = SyncProgressReporter.shared

    var syncID = SyncProgress.SyncID(resource: remappedResource.wrapped, tags: tags)

    let resource = remappedResource.wrapped
    let description = resource.logDescription

    guard self.pauseSynchronization == false else {
      VitalLogger.healthKit.info("[\(description)] skipped (sync paused)", source: "Sync")
      return
    }

    let parkingLot = self.syncSerializerLock.withLock {
      if let parkingLot = self.syncSerializer[remappedResource] {
        return parkingLot
      }

      let newLot = ParkingLot()
      self.syncSerializer[remappedResource] = newLot
      return newLot
    }

    // Use ParkingLot to ensure that — for each `RemappedVitalResource` — there can only be one
    // `sync()` call actually carrying out the sync work.
    //
    // All other subsequent callers would just wait in the ParkingLot until the sync work is done.
    //
    // This defends the SDK against flood of HKObserverQuery callouts, presumably caused by some
    // apps saving each HKSample individually, rather than in batches.
    // (e.g., Oura as at July 2024)
    guard parkingLot.tryTo(.enable) else {
      VitalLogger.healthKit.info("[\(description)] +1 parked; \(tags)", source: "Sync")

      // Throw CancellationError, which we can gracefully ignore.
      try? await parkingLot.parkIfNeeded()

      VitalLogger.healthKit.info("[\(description)] -1 parked; \(tags)", source: "Sync")
      return
    }
    defer { _ = parkingLot.tryTo(.disable) }

    defer { progressStore.flush() }

    guard self.prioritizeSync(remappedResource, tags) else {
      VitalLogger.healthKit.info("[\(description)] skipped (sync deprioritized)", source: "Sync")
      progressStore.recordSync(syncID, .deprioritized)
      return
    }

    guard let configuration = configuration.value else {
      VitalLogger.healthKit.info("[\(description)] configuration unavailable", source: "Sync")
      return
    }

    VitalLogger.healthKit.info("[\(description)] begin \(tags)", source: "Sync")

    let syncAlongside = Self.syncAlongsideTasks[resource].map { action in
      Task { await action(resource) }
    }

    progressStore.recordSync(syncID, .started)
    progressReporter.syncBegin()

    // If we receive this payload in foreground, wrap the sync work in
    // a UIKit background task, in case the user will move the app to background soon.

    let osBackgroundTask: ProtectedBox<UIBackgroundTaskIdentifier>?

    if AppStateTracker.shared.state.status == .foreground {
      osBackgroundTask = ProtectedBox<UIBackgroundTaskIdentifier>()
      osBackgroundTask!.start("vital-sync-\(description)", expiration: {
        progressStore.flush()
      })
      VitalLogger.healthKit.info("started: daily:\(description)", source: "UIKitBgTask")

    } else {
      osBackgroundTask = nil
    }

    // IMPORTANT: Must be called on ALL exit paths below.
    // Pay extra attention when doing early exits with returns and throws.
    func syncEnded(success: Bool) async {
      if let task = syncAlongside {
        // Wait for any sync-alongside task to complete, but we don't care about the result.
        _ = await task.result
      }

      if success {
        scheduleDeprioritizedResourceRetries()
      }

      await progressReporter.syncEnded()

      if let osBackgroundTask = osBackgroundTask {
        osBackgroundTask.endIfNeeded()
        VitalLogger.healthKit.info("ended: daily:\(description)", source: "UIKitBgTask")
      }
    }

    let instruction: SyncInstruction
    let state: LocalSyncState

    do {
      (instruction, state) = try await computeSyncInstruction(remappedResource, syncID: syncID)

      if instruction.stage == .historical {
        syncID.tags.insert(.historicalStage)
      }

    } catch let error {
      VitalLogger.healthKit.info("[\(description)] failed to compute sync instruction; error = \(error)", source: "Sync")
      progressStore.recordSync(syncID, .error)
      await syncEnded(success: false)
      return
    }

    VitalLogger.healthKit.info("[\(description)] \(instruction)", source: "Sync")

    // Signal syncing (so the consumer can convey it to the user)
    _status.send(.syncing(resource))

    @Sendable func readStep(uncommittedAnchors: [StoredAnchor]) async throws -> (ProcessedResourceData?, [StoredAnchor], hasMore: Bool) {
      // Fetch from HealthKit
      let (data, anchors): (ProcessedResourceData?, [StoredAnchor])

      (data, anchors) = try await store.readResource(
        remappedResource,
        instruction,
        AnchorStorageOverlay(wrapped: self.storage, uncommittedAnchors: uncommittedAnchors),
        ReadOptions(perDeviceActivityTS: state.perDeviceActivityTS)
      )

      // Continue the loop if any anchor reports hasMore=true.
      let hasMore = anchors.contains(where: \.hasMore)

      return (data, anchors, hasMore)
    }

    @Sendable func uploadStep(
      data: ProcessedResourceData?,
      anchors: [StoredAnchor],
      hasMore: Bool
    ) async throws -> Int {

      // We skip empty POST only in daily stage.
      // Empty POST is sent for historical stage, so we would consistently emit
      // historical.data.*.created events.
      guard
        let data = data,
        instruction.stage == .historical || data.shouldSkipPost == false
      else {

        VitalLogger.healthKit.info("[\(description)] no data to upload", source: "Sync")
        _status.send(.nothingToSync(resource))

        return 0
      }

      if configuration.mode.isAutomatic {
        VitalLogger.healthKit.info("[\(description)] begin upload: \(instruction.stage)\(data.shouldSkipPost ? ",empty" : "")", source: "Sync")

        // Post data
        try await vitalClient.post(
          data,
          instruction.taggedPayloadStage,
          .appleHealthKit,
          /// We can't use `vitalCalendar` here. We want to send the user's timezone
          /// rather than UTC (which is what `vitalCalendar` is set to).
          TimeZone.current,
          // Is final chunk?
          hasMore == false
        )
      } else {
        VitalLogger.healthKit.info("[\(description)] upload skipped in manual mode", source: "Sync")
      }


      // Save the anchor/date on a succesfull network call
      anchors.forEach(storage.store(entity:))

      // Signal success
      _status.send(.successSyncing(resource, data))

      VitalLogger.healthKit.info("[\(description)] completed: \(hasMore ? "hasMore" : "noMore")", source: "Sync")
      return data.dataCount
    }

    // Overlap read and upload, so we maximize the use of limited background execution time.
    //
    // .read -> .upload
    //           .read  -> .upload
    //                      .read -> .upload
    //                                 (fin)

    let (pipeline, pipelineScheduler) = AsyncStream<PipelineStage>.makeStream()
    pipelineScheduler.yield(.read())

    let uploadSemaphore = ParkingLot().semaphore

    let success = await withTaskGroup(of: Void.self, returning: Bool.self) { group in
      defer { pipelineScheduler.finish() }

      for await stage in pipeline {
        switch stage {
        case let .read(uncommittedAnchors):
          _ = group.addTaskUnlessCancelled {
            let signpost = VitalLogger.Signpost.begin(name: "read", description: description)
            defer { signpost.end() }

            do {
              let (data, anchors, hasMore) = try await readStep(uncommittedAnchors: uncommittedAnchors)
              pipelineScheduler.yield(.upload(data, anchors, hasMore: hasMore))

            } catch is CancellationError {
              pipelineScheduler.yield(.cancelled)

            } catch is TimeoutError {
              pipelineScheduler.yield(.timedOut)

            } catch let error {
              pipelineScheduler.yield(.error(error))
            }
          }

        case let .upload(data, anchors, hasMore: hasMore):
          progressStore.recordSync(syncID, .readChunk)

          _ = group.addTaskUnlessCancelled { [syncID] in
            do {
              try await VitalLogger.Signpost
                .begin(name: "wait-for-outstanding-upload", description: description)
                .endWith {
                  try await uploadSemaphore.acquire()
                }

              defer { uploadSemaphore.release() }

              let signpost2 = VitalLogger.Signpost.begin(name: "upload", description: description)
              defer { signpost2.end() }

              // Schedule an overlapping read
              if hasMore {
                pipelineScheduler.yield(.read(uncommittedAnchors: anchors))
              }

              let dataCount = try await uploadStep(data: data, anchors: anchors, hasMore: hasMore)
              progressStore.recordSync(syncID, dataCount >= 0 ? .uploadedChunk : .noData, dataCount: dataCount)

              if !hasMore {
                pipelineScheduler.yield(.success)
              }

            } catch is CancellationError {
              pipelineScheduler.yield(.cancelled)

            } catch is TimeoutError {
              pipelineScheduler.yield(.timedOut)

            } catch let error {
              pipelineScheduler.yield(.error(error))
            }
          }

        case .cancelled:
          progressStore.recordSync(syncID, .cancelled)
          VitalLogger.healthKit.info("[\(description)] cancelled", source: "Sync")
          return false

        case .timedOut:
          progressStore.recordSync(syncID, .timedOut)
          VitalLogger.healthKit.info("[\(description)] timedOut", source: "Sync")
          return false

        case let .error(error):
          progressStore.recordSync(syncID, .error)
          VitalLogger.healthKit.info("[\(description)] failed; error = \(error)", source: "Sync")
          return false

        case .success:
          self.storage.markHistoricalStageDone(for: remappedResource)
          progressStore.recordSync(syncID, .completed)
          VitalLogger.healthKit.info("[\(description)] completed", source: "Sync")
          return true
        }
      }

      return false
    }

    await syncEnded(success: success)
  }

  private func scheduleDeprioritizedResourceRetries() {
    scope.task(priority: .high) { @MainActor in
      guard AppStateTracker.shared.state.status == .foreground else {
        return
      }

      let deprioritized = SyncProgressStore.shared.get().backfillTypes
        .filter { $0.value.latestSync?.lastStatus == .deprioritized }
        .map { $0.key }
        .compactMap(VitalResource.init)
        .map(VitalHealthKitStore.remapResource(_:))

      // Retry previously deprioritized resources.
      //
      // If their prerequisites still were not satisfied, they will re-enter
      // `syncDeprioritizedQueue` eventually.
      await withTaskGroup(of: Void.self) { group in
        for resource in Set(deprioritized) {
          group.addTask {
            await self.sync(resource, [])
          }
        }
      }
    }
  }

  /// Ask for health data read and write permission from the user.
  ///
  /// The interactive permission prompt is managed by the operating system (Apple iOS). Vital SDK cannot customize
  /// its appearance or behaviour.
  ///
  /// The prompt only appears for resources being asked for the first time. iOS typically ignores any subsequent attempts,
  /// regardless of whether permission has been granted or denied during that first time. You will have to direct your users to
  /// either the Settings ap or the Apple Health app for reviewing and updating the permissions.
  ///
  /// - Parameter readPermissions: `VitalResource`s to request read permission for.
  /// - Parameter writePermissions: `VitalResource`s to request write permission for.
  /// - Parameter extraReadPermissions: Extra HealthKit object types whose read permissions should be requested in addition to the needs of Vital SDK.
  /// - Parameter extraWritePermissions: Extra HealthKit sample types whose write permissions should be requested in addition to the needs of Vital SDK.
  ///
  public func ask(
    readPermissions readResources: [VitalResource],
    writePermissions writeResource: [WritableVitalResource],
    extraReadPermissions: [HKObjectType] = [],
    extraWritePermissions: [HKSampleType] = []
  ) async -> PermissionOutcome {
    
    guard store.isHealthDataAvailable() else {
      return .healthKitNotAvailable
    }
    
    do {
      try await store.requestReadWriteAuthorization(readResources, writeResource, extraReadPermissions, extraWritePermissions)

      let state = authorizationState(store: store)
      SyncProgressStore.shared.recordAsk(state.activeResources)
      SyncProgressStore.shared.flush()

      // We have gone through Ask successfully. Check if a connected source has been created.
      do {
        try await VitalClient.shared.checkConnectedSource(for: .appleHealthKit)

      } catch let error {
        VitalLogger.healthKit.info("proactive CS creation failed; error = \(error)", source: "Ask")
      }

      if configuration.isNil() == false {
        let configuration = await configuration.get()
        
        checkBackgroundUpdates(
          isBackgroundEnabled: configuration.backgroundDeliveryEnabled
        )
        scheduleUnnotifiedResourceRescue()
      }

      return .success
    }
    catch let error {
      return .failure(error.localizedDescription)
    }
  }
  
  public func hasAskedForPermission(resource: VitalResource) -> Bool {
    store.authorizationState(resource).isActive
  }

  public static func healthKitRequirements(for resource: VitalResource) -> HealthKitObjectTypeRequirements {
    return toHealthKitTypes(resource: resource)
  }

  public func dateOfLastSync(for resource: VitalResource) -> Date? {
    guard hasAskedForPermission(resource: resource) else {
      return nil
    }
    
    let requirements = toHealthKitTypes(resource: resource)
    let dates: [Date] = requirements.allObjectTypes.map {
      String(describing: $0.self)
    }.compactMap { key in
      storage.read(key: key)?.date
    }
    
    /// This is not technically correct, because a resource (e.g. activity) can be made up of many types.
    /// In this case, we pick up the most recent one.
    return dates.sorted { $0.compare($1) == .orderedDescending }.first
  }
}

enum BackgroundDeliveryStage {
  case received(BackgroundDeliveryPayload)
  case evaluate
}

enum PipelineStage {
  case read(uncommittedAnchors: [StoredAnchor] = [])
  case upload(ProcessedResourceData?, [StoredAnchor], hasMore: Bool)

  case error(Error)
  case success
  case cancelled
  case timedOut

  var description: String {
    switch self {
    case .read:
      return "read"
    case let .upload(_, _, hasMore):
      return "upload(\(hasMore ? "hasMore" : "noMore"))"
    case .error:
      return "error"
    case .cancelled:
      return "cancelled"
    case .timedOut:
      return "timedOut"
    case .success:
      return "success"
    }
  }
}

extension VitalHealthKitClient {
  public static func read(resource: VitalResource, startDate: Date, endDate: Date) async throws -> ProcessedResourceData? {

    let (data, _): (ProcessedResourceData?, [StoredAnchor]) = try await VitalHealthKit.read(
      resource: VitalHealthKitStore.remapResource(resource),
      healthKitStore:  HKHealthStore(),
      vitalStorage: VitalHealthKitStorage(storage: .debug),
      instruction: SyncInstruction(stage: .daily, query: startDate ..< endDate),
      options: ReadOptions()
    )

    return data
  }
}

extension VitalHealthKitClient {
  public func write(input: DataInput, startDate: Date, endDate: Date) async throws -> Void {
    try await self.store.writeInput(input, startDate, endDate)
  }
  
  public static func write(input: DataInput, startDate: Date, endDate: Date) async throws -> Void {
    let store = HKHealthStore()
    try await VitalHealthKit.write(healthKitStore: store, dataInput: input, startDate: startDate, endDate: endDate)
  }
}

extension VitalHealthKitClient {

  /// Do your own work alongside a sync attempt of a specific VitalResource.
  ///
  /// Note that while your work cannot fail the sync attempt of a VitalResource, the sync attempt will wait for your work to complete
  /// before marking itself as successful, and notifying Apple HealthKit of the work completion.
  ///
  /// - important: You must register all logic before you call `VitalHealthKitClient.automaticConfiguration()` in
  /// your App Delegate.
  public static func syncAlongside(_ resource: VitalResource, action: @escaping (VitalResource) async -> Void) {
    dispatchPrecondition(condition: .onQueue(.main))
    syncAlongsideTasks[resource] = action
  }
}

extension VitalHealthKitClient {
  /// Pause all synchronization, both automatic syncs and any manual `syncData(_:)` calls.
  ///
  /// - note: When unpausing, a sync is automatically triggered on all previously asked-for resources.
  public var pauseSynchronization: Bool {
    get { self.storage.shouldPauseSynchronization() }
    set {
      let oldValue = self.storage.shouldPauseSynchronization()
      self.storage.setPauseSynchronization(newValue)

      // Auto-trigger an asynchronous sync when we un-pause.
      if oldValue && !newValue {
        self.syncData()
      }
    }
  }

  public var syncProgress: SyncProgress {
    SyncProgressStore.shared.get()
  }

  public var syncProgresses: AsyncStream<SyncProgress> {
    AsyncStream<SyncProgress> { continuation in
      let cancellable = syncProgressPublisher()
        .sink { _ in continuation.finish() } receiveValue: { continuation.yield($0) }
      continuation.onTermination = { _ in cancellable.cancel() }
    }
  }

  public func syncProgressPublisher() -> some Publisher<SyncProgress, Never> {
    SyncProgressStore.shared.publisher()
  }

}

extension ProtectedBox<UIBackgroundTaskIdentifier> {
  func start(_ name: String, expiration: @escaping () -> Void) {
    let taskId = UIApplication.shared.beginBackgroundTask(withName: name) { [weak self] in
      expiration()
      self?.endIfNeeded()
    }
    set(value: taskId)
  }

  func endIfNeeded() {
    if let taskId = clean() {
      UIApplication.shared.endBackgroundTask(taskId)
    }
  }
}

private func healthkitCalloutEventType(_ state: AppState.Status) -> SyncProgress.SystemEventType {
  switch state {
  case .background:
    return .healthKitCalloutBackground
  case .foreground:
    return .healthKitCalloutForeground
  case .launching:
    return .healthKitCalloutAppLaunching
  case .terminating:
    return .healthKitCalloutAppTerminating
  }
}

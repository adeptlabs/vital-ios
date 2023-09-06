import HealthKit
import VitalCore

typealias SampleQueryHandler = (HKSampleQuery, [HKSample]?, Error?) -> Void
typealias AnchorQueryHandler = (HKAnchoredObjectQuery, [HKSample]?, [HKDeletedObject]?, HKQueryAnchor?, Error?) -> Void
typealias ActivityQueryHandler = (HKActivitySummaryQuery, [HKActivitySummary]?, Error?) -> Void

typealias SeriesSampleHandler = (HKQuantitySeriesSampleQuery, HKQuantity?, DateInterval?, HKQuantitySample?, Bool, Error?) -> Void

typealias StatisticsHandler = (HKStatisticsCollectionQuery, HKStatisticsCollection?, Error?) -> Void

typealias HourlyStatisticsResultHandler = (Result<[VitalStatistics], Error>) -> Void

enum VitalHealthKitClientError: Error {
  case invalidInterval(Date, Date, context: StaticString)
}

struct VitalStatisticsError: Error {
  let statistics: HKStatistics

  var description: String {
    let formatter = ISO8601DateFormatter()
    let start = formatter.string(from: statistics.startDate)
    let end = formatter.string(from: statistics.endDate)
    return "Failed to convert HKStatistics for \(statistics.quantityType.identifier): \(start) -> \(end)"
  }
}

private func read(
  type: HKSampleType,
  healthKitStore: HKHealthStore,
  vitalStorage: VitalHealthKitStorage,
  typeToResource: ((HKSampleType) -> VitalResource),
  startDate: Date,
  endDate: Date
) async throws -> (ProcessedResourceData?, [StoredAnchor]) {
  func queryQuantities(
    type: HKSampleType
  ) async throws -> (quantities: [QuantitySample], StoredAnchor?) {
    
    let payload = try await query(
      healthKitStore: healthKitStore,
      vitalStorage: vitalStorage,
      type: type,
      startDate: startDate,
      endDate: endDate
    )
    
    let quantities: [QuantitySample] = payload.sample.compactMap(QuantitySample.init)
    return (quantities, payload.anchor)
  }
  
  func queryStatistics(
    type: HKQuantityType
  ) async throws -> (quantities: [QuantitySample], StoredAnchor?) {
    
    let dependencies = StatisticsQueryDependencies.live(
      healthKitStore: healthKitStore,
      vitalStorage: vitalStorage
    )
    
    let payload = try await queryStatisticsSample(
      dependency: dependencies,
      type: type,
      startDate: startDate,
      endDate: endDate
    )
    
    let quantities: [QuantitySample] = payload.statistics.compactMap { value in
      return QuantitySample(value, type)
    }
    
    return (quantities, payload.anchor)
  }
  
  var anchors: [StoredAnchor] = []
  
  switch type {
    case
      /// Activity
      HKSampleType.quantityType(forIdentifier: .activeEnergyBurned)!,
      HKSampleType.quantityType(forIdentifier: .basalEnergyBurned)!,
      HKSampleType.quantityType(forIdentifier: .stepCount)!,
      HKSampleType.quantityType(forIdentifier: .flightsClimbed)!,
      HKSampleType.quantityType(forIdentifier: .distanceWalkingRunning)!:
      
      let (values, anchor) = try await queryStatistics(type: type as! HKQuantityType)
      let patch = ActivityPatch(sampleType: type, samples: values)
      
      anchors.appendOptional(anchor)
      
      return (ProcessedResourceData.summary(.activity(patch)), anchors)
      
    case
      /// Activity
      HKSampleType.quantityType(forIdentifier: .vo2Max)!:
      
      let (values, anchor) = try await queryQuantities(type: type)
      let patch = ActivityPatch(sampleType: type, samples: values)
      
      anchors.appendOptional(anchor)
      
      return (ProcessedResourceData.summary(.activity(patch)), anchors)
      
    case
      /// Body
      HKQuantityType.quantityType(forIdentifier: .bodyMass)!,
      HKQuantityType.quantityType(forIdentifier: .bodyFatPercentage)!:
      
      let (values, anchor) = try await queryQuantities(type: type)
      let patch = BodyPatch(sampleType: type, samples: values)
      
      anchors.appendOptional(anchor)
      
      return (ProcessedResourceData.summary(.body(patch)), anchors)
      
    default:
      return try await read(
        resource: typeToResource(type),
        healthKitStore: healthKitStore,
        typeToResource: typeToResource,
        vitalStorage: vitalStorage,
        startDate: startDate,
        endDate: endDate
      )
  }
}

func read(
  resource: VitalResource,
  healthKitStore: HKHealthStore,
  typeToResource: ((HKSampleType) -> VitalResource),
  vitalStorage: VitalHealthKitStorage,
  startDate: Date,
  endDate: Date
) async throws -> (ProcessedResourceData?, [StoredAnchor]) {
  
  switch resource {
    case .individual:
      
      let types = toHealthKitTypes(resource: resource)
      guard types.count == 1 else {
        fatalError("Individual types should made up of a single type. \(resource) isn't. This is a developer error")
      }
      
      guard let sampleType = types.first as? HKSampleType else {
        fatalError("\(types) is not an HKSampleType")
      }
      
      return try await read(
        type: sampleType,
        healthKitStore: healthKitStore,
        vitalStorage: vitalStorage,
        typeToResource: typeToResource,
        startDate: startDate,
        endDate: endDate
      )
      
    case .profile:
      let profilePayload = try await handleProfile(
        healthKitStore: healthKitStore,
        vitalStorage: vitalStorage
      )

      if let patch = profilePayload.profilePatch {
        return (.summary(.profile(patch)), profilePayload.anchors)
      } else {
        return (nil, [])
      }

      
    case .body:
      let payload = try await handleBody(
        healthKitStore: healthKitStore,
        vitalStorage: vitalStorage,
        startDate: startDate,
        endDate: endDate
      )
      
      return (.summary(.body(payload.bodyPatch)), payload.anchors)
      
    case .sleep:
      let payload = try await handleSleep(
        healthKitStore: healthKitStore,
        vitalStorage: vitalStorage,
        startDate: startDate,
        endDate: endDate
      )
      
      return (.summary(.sleep(payload.sleepPatch)), payload.anchors)
      
    case .activity:
      let payload = try await handleActivity(
        healthKitStore: healthKitStore,
        vitalStorage: vitalStorage,
        startDate: startDate,
        endDate: endDate
      )
      
      return (payload.activityPatch.map { .summary(.activity($0)) }, payload.anchors)
      
    case .workout:
      let payload = try await handleWorkouts(
        healthKitStore: healthKitStore,
        vitalStorage: vitalStorage,
        startDate: startDate,
        endDate: endDate
      )
      
      return (.summary(.workout(payload.workoutPatch)), payload.anchors)
      
    case .vitals(.glucose):
      let payload = try await handleTimeSeries(
        type: .quantityType(forIdentifier: .bloodGlucose)!,
        healthKitStore: healthKitStore,
        vitalStorage: vitalStorage,
        startDate: startDate,
        endDate: endDate
      )
      
      return (.timeSeries(.glucose(payload.samples)), payload.anchors)
      
    case .vitals(.hearthRate):
      let payload = try await handleTimeSeries(
        type: .quantityType(forIdentifier: .heartRate)!,
        healthKitStore: healthKitStore,
        vitalStorage: vitalStorage,
        startDate: startDate,
        endDate: endDate
      )
      
      return (.timeSeries(.heartRate(payload.samples)), payload.anchors)

    case .vitals(.heartRateVariability):
      let payload = try await handleTimeSeries(
        type: .quantityType(forIdentifier: .heartRateVariabilitySDNN)!,
        healthKitStore: healthKitStore,
        vitalStorage: vitalStorage,
        startDate: startDate,
        endDate: endDate
      )

      return (.timeSeries(.heartRateVariability(payload.samples)), payload.anchors)
      
    case .vitals(.bloodPressure):
      let payload = try await handleBloodPressure(
        healthKitStore: healthKitStore,
        vitalStorage: vitalStorage,
        startDate: startDate,
        endDate: endDate
      )
      
      return (.timeSeries(.bloodPressure(payload.bloodPressure)), payload.anchors)
      
    case .nutrition(.water):
      let payload = try await handleTimeSeries(
        type: .quantityType(forIdentifier: .dietaryWater)!,
        healthKitStore: healthKitStore,
        vitalStorage: vitalStorage,
        startDate: startDate,
        endDate: endDate
      )
      
      return (.timeSeries(.nutrition(.water(payload.samples))), payload.anchors)

    case .nutrition(.caffeine):
      let payload = try await handleTimeSeries(
        type: .quantityType(forIdentifier: .dietaryCaffeine)!,
        healthKitStore: healthKitStore,
        vitalStorage: vitalStorage,
        startDate: startDate,
        endDate: endDate
      )

      return (.timeSeries(.nutrition(.caffeine(payload.samples))), payload.anchors)

    case .vitals(.mindfulSession):
      let payload = try await handleMindfulSessions(
        healthKitStore: healthKitStore,
        vitalStorage: vitalStorage,
        startDate: startDate,
        endDate: endDate
      )

      return (.timeSeries(.mindfulSession(payload.samples)), payload.anchors)
  }
}

func handleMindfulSessions(
  healthKitStore: HKHealthStore,
  vitalStorage: VitalHealthKitStorage,
  startDate: Date,
  endDate: Date
) async throws -> (samples: [QuantitySample], anchors: [StoredAnchor]) {

  var anchors: [StoredAnchor] = []

  let payload = try await query(
    healthKitStore: healthKitStore,
    vitalStorage: vitalStorage,
    type: .categoryType(forIdentifier: .mindfulSession)!,
    startDate: startDate,
    endDate: endDate
  )

  let samples = payload.sample.compactMap(QuantitySample.fromMindfulSession(sample:))
  anchors.appendOptional(payload.anchor)

  return (samples: samples, anchors: anchors)
}

func handleProfile(
  healthKitStore: HKHealthStore,
  vitalStorage: VitalHealthKitStorage
) async throws -> (profilePatch: ProfilePatch?, anchors: [StoredAnchor]) {

  let storageKey = "profile"

  let sex = try healthKitStore.biologicalSex().biologicalSex
  let biologicalSex = ProfilePatch.BiologicalSex(healthKitSex: sex)

  let dateOfBirth = try healthKitStore.patched_dateOfBirthComponents()
    .flatMap(vitalCalendar.date(from:))

  let payload: [QuantitySample] = try await querySample(
    healthKitStore: healthKitStore,
    type: .quantityType(forIdentifier: .height)!,
    limit: 1,
    ascending: false
  ).compactMap(QuantitySample.init)
  
  let height = payload.last.map { Int($0.value)}
  
  let profile: ProfilePatch = .init(
    biologicalSex: biologicalSex,
    dateOfBirth: dateOfBirth,
    height: height,
    timeZone: TimeZone.current.identifier
  )
  let id = profile.id

  let anchor = vitalStorage.read(key: storageKey)
  let storedId = anchor?.vitalAnchors?.first?.id

  guard let id = id, storedId != id else {
    return (profilePatch: nil, anchors: [])
  }

  return (
    profilePatch: profile,
    anchors: [.init(key: storageKey, anchor: nil, date: Date(), vitalAnchors: [.init(id: id)])]
  )
}

func handleBody(
  healthKitStore: HKHealthStore,
  vitalStorage: VitalHealthKitStorage,
  startDate: Date,
  endDate: Date
) async throws -> (bodyPatch: BodyPatch, anchors: [StoredAnchor]) {
  
  func queryQuantities(
    type: HKSampleType
  ) async throws -> (quantities: [QuantitySample], StoredAnchor?) {
    
    let payload = try await query(
      healthKitStore: healthKitStore,
      vitalStorage: vitalStorage,
      type: type,
      startDate: startDate,
      endDate: endDate
    )
    
    let quantities: [QuantitySample] = payload.sample.compactMap(QuantitySample.init)
    return (quantities, payload.anchor)
  }
  
  
  var anchors: [StoredAnchor] = []
  
  let (bodyMass, bodyMassAnchor) = try await queryQuantities(
    type: .quantityType(forIdentifier: .bodyMass)!
  )
  
  var (bodyFatPercentage, bodyFatPercentageAnchor) = try await queryQuantities(
    type: .quantityType(forIdentifier: .bodyFatPercentage)!
  )
  
  bodyFatPercentage = bodyFatPercentage.map {
    var copy = $0
    copy.value = $0.value * 100
    return copy
  }
  
  anchors.appendOptional(bodyMassAnchor)
  anchors.appendOptional(bodyFatPercentageAnchor)
  
  return (
    .init(
      bodyMass: bodyMass,
      bodyFatPercentage: bodyFatPercentage
    ),
    anchors
  )
}

func handleSleep(
  healthKitStore: HKHealthStore,
  vitalStorage: VitalHealthKitStorage,
  startDate: Date,
  endDate: Date
) async throws -> (sleepPatch: SleepPatch, anchors: [StoredAnchor]) {
  
  var anchors: [StoredAnchor] = []
  let sleepType = HKSampleType.categoryType(forIdentifier: .sleepAnalysis)!
  
  var predicate: NSPredicate?
  if #available(iOS 16.0, *) {
    predicate = HKCategoryValueSleepAnalysis.predicateForSamples(equalTo: [.asleepUnspecified, .asleepCore, .asleepREM, .asleepDeep, .awake, .inBed])
  }
  
  let payload = try await query(
    healthKitStore: healthKitStore,
    vitalStorage: vitalStorage,
    type: sleepType,
    startDate: startDate,
    endDate: endDate,
    extraPredicate: predicate
  )
  anchors.appendOptional(payload.anchor)

  /// An iPhone is capable of recording sleep. We can see it by looking at the bundle identifier + productType.
  /// However we are only interested in sleep recorded with an Apple Watch.
  /// This filter discards iPhone recorded sleeps.
  ///
  ///
  /// NOTE: We are disabling  this for now. VIT-4156
  //  let sampleWithoutPhone = filterForWatch(samples: payload.sample)
  
  /// The goal of this filter is to remove sleep data generated by 3rd party apps that only do analyses.
  /// Apps like Pillow and SleepWatch are great, but we are only interested in data generated by devices (e.g. Apple Watch, Oura)
  let admittedSamples = filter(samples: payload.sample, by: DataSource.allCases)

  /// Group the sleeps by source bundle before we try to stitch them into sleep sessions.
  let samplesBySourceBundle = Dictionary(
    grouping: admittedSamples,
    by: { SleepGroupKey(bundleIdentifier: $0.sourceRevision.source.bundleIdentifier, productType: $0.sourceRevision.productType) }
  )

  var copies: [SleepPatch.Sleep] = []

  for (groupKey, sampleGroup) in samplesBySourceBundle {
    /// Sorting by start date is essential here, since HKAnchoredObjectQuery returns samples in insertion order.
    /// There is otherwise no guarantee of samples being somewhat chornologically ordered, and being so are important to
    /// achieve consistent and quality stitches across all sorts of HealthKit writers.
    var sleeps = sampleGroup.compactMap(SleepPatch.Sleep.init)
    sleeps.sort { $0.startDate < $1.startDate }

    /// Sleep samples can be sliced up. For example you can have a slice going from `2022-09-08 22:00` to `2022-09-08 22:15`
    /// And several others until the last one is `2022-09-09 07:00`. The goal of of `stitchedSleeps` is to put all these slices together
    /// Into a single `SleepPatch.Sleep` going from `2022-09-08 22:00` to `2022-09-08 22:15`
    let stitchedData = stitchedSleeps(sleeps: sleeps, sourceType: groupKey.sourceType)

    /// `stitchedSleeps` doesn't deal with non-consecutive samples. So it's possible to have a sequence of samples that belong to different
    /// providers (e.g. Apple Watch, Oura, Fitbit). The goal of `mergeSleeps` is to find overlapping sleeps that belong to the same provider and merge them
    let mergedData = mergeSleeps(sleeps: stitchedData)

    for var sleep in mergedData {
      func fitSamples(
        sleep: inout SleepPatch.Sleep
      ) {
        for filteredSample in sampleGroup {
          if let categorySample = filteredSample as? HKCategorySample,
             let sleepAnalysis = HKCategoryValueSleepAnalysis(rawValue: categorySample.value),
             filteredSample.sourceRevision.productType == sleep.productType &&
              filteredSample.sourceRevision.source.bundleIdentifier == sleep.sourceBundle &&
              filteredSample.startDate >= sleep.startDate && filteredSample.endDate <= sleep.endDate

          {
            let sample = QuantitySample(categorySample: categorySample)
            switch sleepAnalysis {
            case .asleepUnspecified:
              sleep.sleepStages.unspecifiedSleepSamples.append(sample)

            case .awake:
              sleep.sleepStages.awakeSleepSamples.append(sample)

            case .asleepCore:
              sleep.sleepStages.lightSleepSamples.append(sample)

            case .asleepREM:
              sleep.sleepStages.remSleepSamples.append(sample)

            case .asleepDeep:
              sleep.sleepStages.deepSleepSamples.append(sample)

            case .inBed:
              sleep.sleepStages.inBedSleepSamples.append(sample)

            @unknown default:
              sleep.sleepStages.unknownSleepSamples.append(sample)
            }
          }
        }
      }

      if #available(iOS 16.0, *) {
        fitSamples(sleep: &sleep)
      }

      let originalHR: [HKSample] = try await querySample(
        healthKitStore: healthKitStore,
        type: .quantityType(forIdentifier: .heartRate)!,
        startDate: sleep.startDate,
        endDate: sleep.endDate
      )

      let filteredHR = originalHR.filter(by: sleep.sourceBundle)
      let heartRate = filteredHR.compactMap(QuantitySample.init)

      let hearRateVariability: [QuantitySample] = try await querySample(
        healthKitStore: healthKitStore,
        type: .quantityType(forIdentifier: .heartRateVariabilitySDNN)!,
        startDate: sleep.startDate,
        endDate: sleep.endDate
      )
        .filter(by: sleep.sourceBundle)
        .compactMap(QuantitySample.init)

      let oxygenSaturation: [QuantitySample] = try await querySample(
        healthKitStore: healthKitStore,
        type: .quantityType(forIdentifier: .oxygenSaturation)!,
        startDate: sleep.startDate,
        endDate: sleep.endDate
      )
        .filter(by: sleep.sourceBundle)
        .compactMap(QuantitySample.init)

      let restingHeartRate: [QuantitySample] = try await querySample(
        healthKitStore: healthKitStore,
        type: .quantityType(forIdentifier: .restingHeartRate)!,
        startDate: sleep.startDate,
        endDate: sleep.endDate
      )
        .filter(by: sleep.sourceBundle)
        .compactMap(QuantitySample.init)

      let respiratoryRate: [QuantitySample] = try await querySample(
        healthKitStore: healthKitStore,
        type: .quantityType(forIdentifier: .respiratoryRate)!,
        startDate: sleep.startDate,
        endDate: sleep.endDate
      )
        .filter(by: sleep.sourceBundle)
        .compactMap(QuantitySample.init)

      let wristTemperature: [QuantitySample]

      if #available(iOS 16.0, *) {
        wristTemperature = try await querySample(
          healthKitStore: healthKitStore,
          type: .quantityType(forIdentifier: .appleSleepingWristTemperature)!,
          startDate: sleep.startDate,
          endDate: sleep.endDate,
          // `appleSleepingWristTemperature` sometimes falls outside the bounds of a sleep
          // This means that if we use `strictStartDate` we won't pick up these values.
          options: []
        )
        .filter(by: sleep.sourceBundle)
        .compactMap(QuantitySample.init)
      } else {
        wristTemperature = []
      }

      var copy = sleep
      copy.heartRate = heartRate
      copy.heartRateVariability = hearRateVariability
      copy.restingHeartRate = restingHeartRate
      copy.oxygenSaturation = oxygenSaturation
      copy.respiratoryRate = respiratoryRate
      copy.wristTemperature = wristTemperature

      copies.append(copy)
    }
  }

  return (.init(sleep: copies), anchors)
}

func handleActivity(
  healthKitStore: HKHealthStore,
  vitalStorage: VitalHealthKitStorage,
  startDate: Date,
  endDate: Date
) async throws -> (activityPatch: ActivityPatch?, anchors: [StoredAnchor]) {

  let dependencies = StatisticsQueryDependencies.live(
    healthKitStore: healthKitStore,
    vitalStorage: vitalStorage
  )

  // Calendar in local TZ, used for day summaries and final sample grouping.
  let deviceTimeZoneCalendar = GregorianCalendar(timeZone: .current)

  func queryQuantities(
    type: HKSampleType
  ) async throws -> (quantities: [QuantitySample], StoredAnchor?) {
    
    let payload = try await query(
      healthKitStore: healthKitStore,
      vitalStorage: vitalStorage,
      type: type,
      startDate: startDate,
      endDate: endDate
    )
    
    let quantities: [QuantitySample] = payload.sample.compactMap(QuantitySample.init)
    return (quantities, payload.anchor)
  }

  func queryHourlyStatistics(
    type: HKQuantityType
  ) async throws -> (quantities: [QuantitySample], StoredAnchor?) {

    let payload = try await queryStatisticsSample(
      dependency: dependencies,
      type: type,
      startDate: startDate,
      endDate: endDate
    )

    let quantities: [QuantitySample] = payload.statistics.compactMap { value in
      return QuantitySample(value, type)
    }

    return (quantities, payload.anchor)
  }

  // - Hourly timeseries samples
  var anchors: [StoredAnchor] = []
  
  let (activeEnergyBurned, activeEnergyBurnedAnchor) = try await queryHourlyStatistics(
    type: .quantityType(forIdentifier: .activeEnergyBurned)!
  )
  
  let (basalEnergyBurned, basalEnergyBurnedAnchor) = try await queryHourlyStatistics(
    type: .quantityType(forIdentifier: .basalEnergyBurned)!
  )
  
  let (steps, stepsAnchor) = try await queryHourlyStatistics(
    type: .quantityType(forIdentifier: .stepCount)!
  )
  
  let (floorsClimbed, floorsClimbedAnchor) = try await queryHourlyStatistics(
    type: .quantityType(forIdentifier: .flightsClimbed)!
  )
  
  let (distanceWalkingRunning, distanceWalkingRunningAnchor) = try await queryHourlyStatistics(
    type: .quantityType(forIdentifier: .distanceWalkingRunning)!
  )
  
  let (vo2Max, vo2MaxAnchor) = try await queryQuantities(
    type: .quantityType(forIdentifier: .vo2Max)!
  )
  
  anchors.appendOptional(activeEnergyBurnedAnchor)
  anchors.appendOptional(basalEnergyBurnedAnchor)
  anchors.appendOptional(stepsAnchor)
  anchors.appendOptional(floorsClimbedAnchor)
  anchors.appendOptional(distanceWalkingRunningAnchor)
  anchors.appendOptional(vo2MaxAnchor)

  // - Day summaries

  // Find the minimum start date.
  let daySummaryStartDate: Date? = [
    activeEnergyBurned.first?.startDate,
    basalEnergyBurned.first?.startDate,
    steps.first?.startDate,
    floorsClimbed.first?.startDate,
    distanceWalkingRunning.first?.startDate,
  ].compactMap { $0 }.min()

  let daySummaries: [GregorianCalendar.FloatingDate: ActivityPatch.DaySummary]

  if let startDate = daySummaryStartDate {
    daySummaries = try await queryActivityDaySummaries(
      dependencies: dependencies,
      startTime: daySummaryStartDate ?? startDate,
      endTime: endDate,
      in: deviceTimeZoneCalendar
    )
  } else {
    daySummaries = [:]
  }

  let allSamples = ActivityPatch.Activity(
    activeEnergyBurned: activeEnergyBurned,
    basalEnergyBurned: basalEnergyBurned,
    steps: steps,
    floorsClimbed: floorsClimbed,
    distanceWalkingRunning: distanceWalkingRunning,
    vo2Max: vo2Max
  )

  let patch = activityPatchGroupedByDay(summaries: daySummaries, samples: allSamples, in: deviceTimeZoneCalendar)
  
  return (patch, anchors: anchors)
}

func handleWorkouts(
  healthKitStore: HKHealthStore,
  vitalStorage: VitalHealthKitStorage,
  startDate: Date,
  endDate: Date
) async throws -> (workoutPatch: WorkoutPatch, anchors: [StoredAnchor]) {
  
  var anchors: [StoredAnchor] = []
  
  let payload = try await query(
    healthKitStore: healthKitStore,
    vitalStorage: vitalStorage,
    type: .workoutType(),
    startDate: startDate,
    endDate: endDate
  )
  
  let workouts = payload.sample.compactMap(WorkoutPatch.Workout.init)
  anchors.appendOptional(payload.anchor)
  
  var copies: [WorkoutPatch.Workout] = []
  
  for workout in workouts {
    let heartRate: [QuantitySample] = try await querySample(
      healthKitStore: healthKitStore,
      type: .quantityType(forIdentifier: .heartRate)!,
      startDate: workout.startDate,
      endDate: workout.endDate
    )
      .filter(by: workout.sourceBundle)
      .compactMap(QuantitySample.init)
    
    let respiratoryRate: [QuantitySample] = try await querySample(
      healthKitStore: healthKitStore,
      type: .quantityType(forIdentifier: .respiratoryRate)!,
      startDate: workout.startDate,
      endDate: workout.endDate
    )
      .filter(by: workout.sourceBundle)
      .compactMap(QuantitySample.init)
    
    var copy = workout
    copy.heartRate = heartRate
    copy.respiratoryRate = respiratoryRate
    
    copies.append(copy)
  }
  
  return (.init(workouts: copies), anchors)
}


func handleBloodPressure(
  healthKitStore: HKHealthStore,
  vitalStorage: VitalHealthKitStorage,
  startDate: Date,
  endDate: Date
) async throws -> (bloodPressure: [BloodPressureSample], anchors: [StoredAnchor]) {
  
  let bloodPressureIdentifier = HKCorrelationType.correlationType(forIdentifier: .bloodPressure)!
  
  let payload = try await query(
    healthKitStore: healthKitStore,
    vitalStorage: vitalStorage,
    type: bloodPressureIdentifier,
    startDate: startDate,
    endDate: endDate
  )
  
  var anchors: [StoredAnchor] = []
  let bloodPressure: [BloodPressureSample] = payload.sample.compactMap(BloodPressureSample.init)
  
  anchors.appendOptional(payload.anchor)
  
  return (bloodPressure: bloodPressure, anchors: anchors)
}

func handleTimeSeries(
  type: HKSampleType,
  healthKitStore: HKHealthStore,
  vitalStorage: VitalHealthKitStorage,
  startDate: Date,
  endDate: Date
) async throws -> (samples: [QuantitySample], anchors: [StoredAnchor]) {
  
  let payload = try await query(
    healthKitStore: healthKitStore,
    vitalStorage: vitalStorage,
    type: type,
    startDate: startDate,
    endDate: endDate
  )
  
  var anchors: [StoredAnchor] = []
  let samples: [QuantitySample] = payload.sample.compactMap(QuantitySample.init)
  
  anchors.appendOptional(payload.anchor)
  
  return (samples,anchors)
}


private func query(
  healthKitStore: HKHealthStore,
  vitalStorage: VitalHealthKitStorage,
  type: HKSampleType,
  limit: Int = HKObjectQueryNoLimit,
  startDate: Date? = nil,
  endDate: Date? = nil,
  extraPredicate: NSPredicate? = nil
) async throws -> (sample: [HKSample], anchor: StoredAnchor?) {
  
  return try await withCheckedThrowingContinuation { continuation in
    
    let handler: AnchorQueryHandler = { (query, samples, deletedObject, newAnchor, error) in
      healthKitStore.stop(query)
      
      if let error = error {
        continuation.resume(with: .failure(error))
        return
      }
      
      let storedAnchor = StoredAnchor(
        key: String(describing: type),
        anchor: newAnchor,
        date: Date(),
        vitalAnchors: nil
      )
      
      continuation.resume(with: .success((samples ?? [], storedAnchor)))
    }
    
    var predicate = HKQuery.predicateForSamples(withStart: startDate, end: endDate, options: [])
    
    if let extraPredicate = extraPredicate {
      predicate = NSCompoundPredicate(andPredicateWithSubpredicates: [predicate, extraPredicate])
    }
    
    let anchor = vitalStorage.read(key: String(describing: type.self))?.anchor
    
    let query = HKAnchoredObjectQuery(
      type: type,
      predicate: predicate,
      anchor: anchor,
      limit: limit,
      resultsHandler: handler
    )
    
    healthKitStore.execute(query)
  }
}

func calculateIdsForAnchorsPopulation(
  vitalStatistics: [VitalStatistics],
  date: Date
) -> [VitalAnchor] {
  let cleanedStatistics = vitalStatistics.filter { statistic in
    isValidStatistic(statistic)
  }
  
  /// Generate new anchors based on the read statistics values
  let newAnchors = cleanedStatistics.compactMap { statistics in
    generateIdForAnchor(statistics).map(VitalAnchor.init(id:))
  }
  
  let value = Set(newAnchors)
  return Array(value)
}

func calculateIdsForAnchorsAndData(
  vitalStatistics: [VitalStatistics],
  existingAnchors: [VitalAnchor],
  key: String,
  date: Date
) -> ([VitalStatistics], StoredAnchor) {
  
  /// Clean-up the values with zero
  let cleanedStatistics = vitalStatistics.filter { statistic in
    isValidStatistic(statistic)
  }
  
  /// Generate new anchors based on the read statistics values
  let newAnchors = cleanedStatistics.compactMap { statistics in
    generateIdForAnchor(statistics).map(VitalAnchor.init(id:))
  }
    
  /// There's a difference between what we want to send to the server, versus what we want to store
  /// The ones to send, is the delta between the new versus the existing.
  let toSendIds = anchorsToSend(old: existingAnchors, new: newAnchors)
  
  /// The ones we want store is a union of all ids.
  let toStoreIds = anchorsToStore(old: existingAnchors, new: newAnchors)
  
  /// We can now filter the statistics that match the ids we want to send
  let dataToSend: [VitalStatistics] = cleanedStatistics.filter { statistics in
    guard let id = generateIdForAnchor(statistics) else {
      return false
    }
    
    return toSendIds.map(\.id).contains(id)
  }
  
  let storedAnchor = StoredAnchor(
    key: key,
    anchor: nil,
    date: date,
    vitalAnchors: toStoreIds
  )

  return (dataToSend, storedAnchor)
}


/// TODO: To be Removed
private func populateAnchorsForStatisticalQuery(
  dependency: StatisticsQueryDependencies,
  type: HKQuantityType,
  statisticsQueryStartDate: Date
) async throws -> [VitalAnchor] {
    
  let startDate = vitalCalendar.date(byAdding: .day, value: -21, to: statisticsQueryStartDate)!.dayStart
  let endDate = statisticsQueryStartDate.beginningHour

  let statistics = try await dependency.executeStatisticalQuery(type, startDate ..< endDate, .hourly)
  let newAnchors = calculateIdsForAnchorsPopulation(
    vitalStatistics: statistics,
    date: endDate
  )

  return newAnchors
}

///
///
///                  `populateAnchorsForStatisticalQuery`
///           |______________________________________________|
///   `populateAnchorsStart`                          `lastSavedDate`
///
///                                                           <------------>
///                                                              delta
///
///
///                                         |_______________________________|
///                                `statisticsStarDate`                  `nextHour`
///
///
///  The goal with `populateAnchorsForStatisticalQuery` is to fill up our stored anchors.
///  For a new user in the system, it's nil. And if they are indeed new, we won't do anything.
///  We will only call `populateAnchorsForStatisticalQuery` for users still using `date anchors`.
///  We then try to get data between `statisticsStarDate` and `nextHour`. If the mechanism
///  is working as expected, most values are inside the `delta` range. However
///  it's still possible to catch scatered values between `statisticsStarDate` and `nextHour`.
///  This happens when a app syncs up with the HealthApp before the `lastSavedDate`.
///
func queryStatisticsSample(
  dependency: StatisticsQueryDependencies,
  type: HKQuantityType,
  startDate: Date,
  endDate: Date
) async throws -> (statistics: [VitalStatistics], anchor: StoredAnchor) {
  guard startDate <= endDate else {
    throw VitalHealthKitClientError.invalidInterval(startDate, endDate, context: #function)
  }

  let vitalAnchors: [VitalAnchor]
  
  var newStartDate = dependency.storedDate(type) ?? startDate
  var newEndDate = endDate

  // System wall clock could potentially roll backward/forward significantly,
  // so let's sanitize any persisted dates before proceeding.
  newStartDate = min(newStartDate, newEndDate)
  newEndDate = max(newStartDate, newEndDate)

  let isFirstTimeSycingType = dependency.isFirstTimeSycingType(type)
  let isLegacyType = dependency.isLegacyType(type)
    
  ///  TODO: Remove this in the near future
  /// <TO BE REMOVED>
  if isLegacyType {
    /// We will fill up 21 days worth of anchors
    vitalAnchors = try await populateAnchorsForStatisticalQuery(
      dependency: dependency,
      type: type,
      statisticsQueryStartDate: newStartDate
    )
  } else {
    vitalAnchors = dependency.vitalAnchorsForType(type)
  }
  ///
  /// </TO BE REMOVED>
  ///
  ///
  
  /// Because there are no anchors, we might miss data that was inserted before our "date anchor".
  /// E.g. Anchor date is at 16:00. Another app (e.g. Garmin) inserts steps at 15:00. If we don't check at least 7 days
  /// before, we might run the risk of losing data inserted before the date anchor
  /// We might need to extend this value to a week.
  let statisticsStartDate: Date
  
  if isFirstTimeSycingType {
    /// If it's the first time, it will try to fetch 30 days worth of data (or whatever `withStart` is)
    statisticsStartDate = newStartDate.dayStart
  } else {
    /// If it's not, we only check 7 days.
    statisticsStartDate = Date.dateAgo(newStartDate, days: 7)
  }

  let queryInterval = statisticsStartDate ..< newEndDate

  let (statistics, anchor) = calculateIdsForAnchorsAndData(
    vitalStatistics: try await dependency.executeStatisticalQuery(type, queryInterval, .hourly),
    existingAnchors: vitalAnchors,
    key: dependency.key(type),
    date: newEndDate
  )

  return (statistics, anchor)
}

/// We compute one summary per quantity type for the calendar day in the
/// **CURRENT DEVICE TIME ZONE**. After all, a (floating) day cannot be
/// projected into UTC time without a time zone, and the user intuition is
/// to see numbers and time that align to their perception of time.
///
/// (where the device time zone is the closest approximation)
///
/// This is different from the typical hourly timeseries samples gathering process, which
/// works with `HKStatisticalCollectionQuery` solely in UTC time (`vitalCalendar`).
/// Hour granularity is time zone agnostic, and the resulting hourly samples can be reinterpreted into all
/// time zones pretty easily and consistently.
func queryActivityDaySummaries(
  dependencies: StatisticsQueryDependencies,
  startTime: Date,
  endTime: Date,
  in calendar: GregorianCalendar
) async throws -> [GregorianCalendar.FloatingDate: ActivityPatch.DaySummary] {
  // System wall time can go backwards. Cap the start time just in case.
  let datesToCompute = calendar.floatingDate(of: min(startTime, endTime))
    ... calendar.floatingDate(of: endTime)
  let queryInterval = calendar.timeRange(of: datesToCompute)

  VitalLogger.healthKit.info("""
  [Day Summary] lastComputed = \(startTime, privacy: .public) now = \(endTime, privacy: .public)
  datesToCompute = \(datesToCompute.lowerBound, privacy: .public) ... \(datesToCompute.upperBound, privacy: .public)
  queryInterval = \(queryInterval.lowerBound, privacy: .public) ..< \(queryInterval.upperBound, privacy: .public)
  """)

  async let _activeEnergyBurnedSum = dependencies.executeStatisticalQuery(
    HKQuantityType.quantityType(forIdentifier: .activeEnergyBurned)!,
    queryInterval,
    .daily
  )
  async let _basalEnergyBurnedSum = dependencies.executeStatisticalQuery(
    HKQuantityType.quantityType(forIdentifier: .basalEnergyBurned)!,
    queryInterval,
    .daily
  )
  async let _stepsSum = dependencies.executeStatisticalQuery(
    HKQuantityType.quantityType(forIdentifier: .stepCount)!,
    queryInterval,
    .daily
  )
  async let _floorsClimbedSum = dependencies.executeStatisticalQuery(
    HKQuantityType.quantityType(forIdentifier: .flightsClimbed)!,
    queryInterval,
    .daily
  )
  async let _distanceWalkingRunningSum = dependencies.executeStatisticalQuery(
    HKQuantityType.quantityType(forIdentifier: .distanceWalkingRunning)!,
    queryInterval,
    .daily
  )

  func keyedByDate(_ statistics: [VitalStatistics]) -> [GregorianCalendar.FloatingDate: VitalStatistics] {
    Dictionary(grouping: statistics) { calendar.floatingDate(of: $0.startDate) }
      .mapValues { statistics in
        assert(statistics.count == 1, "Expected statistical query to produce one stat per day. Found multiple stats per day.")
        return statistics[0]
      }
  }

  let activeEnergyBurnedSum = keyedByDate(try await _activeEnergyBurnedSum)
  let basalEnergyBurnedSum = keyedByDate(try await _basalEnergyBurnedSum)
  let stepsSum = keyedByDate(try await _stepsSum)
  let floorsClimbedSum = keyedByDate(try await _floorsClimbedSum)
  let distanceWalkingRunningSum = keyedByDate(try await _distanceWalkingRunningSum)

  var result = [GregorianCalendar.FloatingDate: ActivityPatch.DaySummary]()
  for date in calendar.enumerate(datesToCompute) {
    // Round down all sums to match Apple Health app behaviour.
    result[date] = ActivityPatch.DaySummary(
      calendarDate: date,
      activeEnergyBurnedSum: activeEnergyBurnedSum[date]?.value.rounded(.down),
      basalEnergyBurnedSum: basalEnergyBurnedSum[date]?.value.rounded(.down),
      stepsSum: (stepsSum[date]?.value.rounded(.down)).map(Int.init),
      floorsClimbedSum: (floorsClimbedSum[date]?.value.rounded(.down)).map(Int.init),
      distanceWalkingRunningSum: distanceWalkingRunningSum[date]?.value.rounded(.down)
    )
  }
  return result
}

func querySample(
  healthKitStore: HKHealthStore,
  type: HKSampleType,
  limit: Int = HKObjectQueryNoLimit,
  startDate: Date? = nil,
  endDate: Date? = nil,
  ascending: Bool = true,
  options: HKQueryOptions = [.strictStartDate]
) async throws -> [HKSample] {
  
  return try await withCheckedThrowingContinuation { continuation in
    
    let handler: SampleQueryHandler = { (query, samples, error) in
      healthKitStore.stop(query)
      
      if let error = error {
        continuation.resume(with: .failure(error))
        return
      }
      
      continuation.resume(with: .success(samples ?? []))
    }
    
    let predicate = HKQuery.predicateForSamples(
      withStart: startDate,
      end: endDate,
      options: options
    )
    
    let sort = [
      NSSortDescriptor(key: HKSampleSortIdentifierStartDate, ascending: ascending)
    ]
    
    let query = HKSampleQuery(
      sampleType: type,
      predicate: predicate,
      limit: limit,
      sortDescriptors: sort,
      resultsHandler: handler
    )
    
    healthKitStore.execute(query)
  }
}

func mergeSleeps(sleeps: [SleepPatch.Sleep]) -> [SleepPatch.Sleep] {
  
  func compareSleep(
    sleeps: [SleepPatch.Sleep],
    sleep: SleepPatch.Sleep
  ) -> [SleepPatch.Sleep] {
    
    for (index, value) in sleeps.enumerated() {
      
      if
        (value.startDate ... value.endDate).overlaps(sleep.startDate ... sleep.endDate) &&
          value.sourceBundle == sleep.sourceBundle
      {
        
        var copySleep = sleep
        copySleep.startDate = min(value.startDate, copySleep.startDate)
        copySleep.endDate = max(value.endDate, copySleep.endDate)
        
        var copySleeps = sleeps
        copySleeps.remove(at: index)
        
        return compareSleep(sleeps: copySleeps, sleep: copySleep)
      }
    }
    
    return sleeps + [sleep]
  }
  
  let sanityCheck = sleeps.filter {
    $0.endDate >= $0.startDate
  }
  
  return sanityCheck.reduce(into: []) { acc, sleep in
    acc = compareSleep(sleeps: acc, sleep: sleep)
  }
}

func stitchedSleeps(sleeps: [SleepPatch.Sleep], sourceType: SourceType) -> [SleepPatch.Sleep] {
  let allowedGapInMinutes: Double
  switch sourceType {
  case .phone:
    /// iPhone sleep has only inBed samples, while gaps are implying "awake".
    /// So we need a more generous time gap allowance when stitching sleeps.
    allowedGapInMinutes = 120
  default:
    allowedGapInMinutes = 30
  }

  return sleeps.reduce(into: []) { acc, newSleep in
    
    guard var lastValue = acc.last else {
      acc = [newSleep]
      return
    }
    
    guard lastValue.sourceBundle == newSleep.sourceBundle else {
      acc = acc + [newSleep]
      return
    }
    
    /// A) `newSleep` happens after `lastValue`
    ///
    /// |____________|   <->  |____________|
    ///    lastValue     X min     newSleep
    if newSleep.startDate > lastValue.endDate  {
      
      /// It happens in less than X minutes
      if isDuration(lastValue.endDate, newSleep.startDate, longerThan: allowedGapInMinutes * 60) == false {
        let newAcc = acc.dropLast()
        lastValue.endDate = newSleep.endDate
        acc = newAcc + [lastValue]
        return
      }
    }
    
    /// B) `lastValue` happens after `newSleep`
    ///
    /// |____________|  <->  |____________|
    ///    newSleep     X min     lastValue
    if lastValue.startDate > newSleep.endDate  {
      let longerThanAllowed = isDuration(newSleep.endDate, lastValue.startDate, longerThan: allowedGapInMinutes * 60)
      
      /// It happens in less than X minutes
      if longerThanAllowed == false {
        let newAcc = acc.dropLast()
        lastValue.startDate = newSleep.startDate
        acc = newAcc + [lastValue]
        return
      }
    }
    
    /// C) `lastValue` overlaps `newSleep` (and vice-versa)
    ///
    /// |____________|
    ///    newSleep
    ///       |____________|
    ///         lastValue
    
    if (lastValue.startDate ... lastValue.endDate).overlaps(newSleep.startDate ... newSleep.endDate) {
      let newAcc = acc.dropLast()
      
      lastValue.startDate = min(lastValue.startDate, newSleep.startDate)
      lastValue.endDate = max(lastValue.endDate, newSleep.endDate)
      
      acc = newAcc + [lastValue]
      return
    }
    
    /// There's no overlap, or the difference is more than 30 minutes.
    /// It's likely a completely new entry
    acc = acc + [newSleep]
  }
}

private func filterForWatch(samples: [HKSample]) -> [HKSample] {
  return samples.filter { sample in
    let bundleIdentifier = sample.sourceRevision.source.bundleIdentifier
    
    if bundleIdentifier.contains(DataSource.appleHealthKit.rawValue) {
      
      /// Data generated from a watch can look like this `Optional("Watch4,4")`
      if let productType = sample.sourceRevision.productType {
        return productType.lowercased().contains("watch")
      } else {
        /// We don't know what this is.
        return false
      }
      
    } else {
      /// This sample was made by something else besides com.apple.healthkit
      /// So we allow it
      return true
    }
  }
}

private func filter(samples: [HKSample], by dataSources: [DataSource]) -> [HKSample] {
  return samples.filter { sample in
    let identifier = sample.sourceRevision.source.bundleIdentifier
    
    for dataSource in dataSources {
      if identifier.contains(dataSource.rawValue) {
        return true
      }
    }
    
    return false
  }
}

private func filter(samples: [HKSample], on productType: String, sourceBundle: String) -> [HKSample] {
  return samples.filter { sample in
    return sample.sourceRevision.productType == productType &&
    sample.sourceRevision.source.bundleIdentifier == sourceBundle
  }
}

private func isDuration(_ date1: Date, _ date2: Date, longerThan seconds: TimeInterval) -> Bool {
  let diff = abs(date1.timeIntervalSinceReferenceDate - date2.timeIntervalSinceReferenceDate)
  return diff > seconds
}

private func orderByDate(_ values: [QuantitySample]) -> [QuantitySample] {
  return values.sorted { $0.startDate < $1.startDate }
}

func splitPerBundle(_ values: [QuantitySample]) -> [[QuantitySample]] {
  var temp: [String: [QuantitySample]] = ["na": []]
  
  for value in values {
    if let bundle = value.sourceBundle {
      if temp[bundle] != nil {
        var copy = temp[bundle]!
        copy.append(value)
        temp[bundle] = copy
      } else {
        temp[bundle] = [value]
      }
    } else {
      var copy = temp["na"]!
      copy.append(value)
      temp["na"] = copy
    }
  }
  
  var outcome: [[QuantitySample]] = [[]]
  
  for key in temp.keys {
    let samples = temp[key]!
    outcome.append(samples)
  }
  
  return outcome
}

private func _accumulate(_ values: [QuantitySample], interval: Int, calendar: Calendar) -> [QuantitySample] {
  let ordered = orderByDate(values)
  
  return ordered.reduce(into: []) { newValues, newValue in
    if let lastValue = newValues.last {
      
      let sameHour = calendar.isDate(newValue.startDate, equalTo: lastValue.startDate, toGranularity: .hour)
      
      guard sameHour else {
        newValues.append(newValue)
        return
      }
      
      let lastValueBucket = Int(calendar.component(.minute, from: lastValue.startDate) / interval)
      let newValueBucket = Int(calendar.component(.minute, from: newValue.startDate) / interval)
      
      guard lastValueBucket == newValueBucket else {
        newValues.append(newValue)
        return
      }
      
      var lastValue = newValues.removeLast()
      lastValue.value = lastValue.value + newValue.value
      lastValue.endDate = newValue.endDate
      
      newValues.append(lastValue)
      
    } else {
      newValues.append(newValue)
    }
  }
}

func accumulate(_ values: [QuantitySample], interval: Int = 15, calendar: Calendar) -> [QuantitySample] {
  let split = splitPerBundle(values)
  let outcome = split.flatMap {
    _accumulate($0, interval: interval, calendar: calendar)
  }
  
  return orderByDate(outcome)
}

private func _average(_ values: [QuantitySample], calendar: Calendar) -> [QuantitySample] {
  let ordered = orderByDate(values)
  
  guard ordered.count > 2 else {
    return values
  }
  
  /// We want to preserve these values, instead of averaging them.
  var min: QuantitySample? = nil
  var max: QuantitySample? = nil
  
  for value in values {
    if let minValue = min {
      if value.value <= minValue.value {
        min = value
      }
    } else {
      min = value
    }
    
    if let maxValue = max {
      if value.value >= maxValue.value {
        max = value
      }
    } else {
      max = value
    }
  }
  
  class Payload {
    var samples: [QuantitySample] = []
    var total: Double = 0
    var totalGrouped: Double = 0
    var count = 0
  }
  
  var outcome: [QuantitySample] = ordered.reduce(into: Payload()) { payload, newValue in
    payload.count = payload.count + 1
    
    if let lastValue = payload.samples.last {
      
      let time = calendar.date(byAdding: .second, value: 5, to: lastValue.startDate)!
      if time >= newValue.startDate {
        var lastValue = payload.samples.removeLast()
        
        payload.total = payload.total + newValue.value
        payload.totalGrouped = payload.totalGrouped + 1
        lastValue.endDate = newValue.endDate
        
        if payload.count == ordered.count {
          lastValue.value = payload.total / payload.totalGrouped
        }
        
        payload.samples.append(lastValue)
      } else {
        var lastValue = payload.samples.removeLast()
        lastValue.value = payload.total / payload.totalGrouped
        payload.samples.append(lastValue)
        
        payload.total = newValue.value
        payload.totalGrouped = 1
        
        payload.samples.append(newValue)
      }
      
    } else {
      payload.total = newValue.value
      payload.totalGrouped = payload.totalGrouped + 1
      payload.samples.append(newValue)
    }
  }.samples
  
  if let max = max {
    outcome.append(max)
  }
  
  if let min = min {
    outcome.append(min)
  }
  
  return outcome
}

func average(_ values: [QuantitySample], calendar: Calendar) -> [QuantitySample] {
  let split = splitPerBundle(values)
  let outcome = split.flatMap {
    _average($0, calendar: calendar)
  }
  
  return orderByDate(outcome)
}

func activityPatchGroupedByDay(
  summaries: [GregorianCalendar.FloatingDate: ActivityPatch.DaySummary],
  samples: ActivityPatch.Activity,
  in calendar: GregorianCalendar
) -> ActivityPatch? {
  func grouped(_ samples: [QuantitySample]) -> [GregorianCalendar.FloatingDate: [QuantitySample]] {
    Dictionary(
     grouping: samples, by: { calendar.floatingDate(of: $0.startDate) }
   )
  }

  let activeEnergyBurned = grouped(samples.activeEnergyBurned)
  let basalEnergyBurned = grouped(samples.basalEnergyBurned)
  let steps = grouped(samples.steps)
  let distanceWalkingRunning = grouped(samples.distanceWalkingRunning)
  let floorsClimbed = grouped(samples.floorsClimbed)
  let vo2Max = grouped(samples.vo2Max)

  // Current assumption: Day summaries must have been computed from the earliest calendar date
  // touched by the samples we've loaded. So `summaries.key` must contain all the dates that have
  // data.
  let activities = summaries
    .filter { $0.value.isNotEmpty }
    .sorted(by: { $0.key < $1.key })
    .map { date, summary in
      ActivityPatch.Activity(
        daySummary: summary,
        activeEnergyBurned: activeEnergyBurned[date] ?? [],
        basalEnergyBurned: basalEnergyBurned[date] ?? [],
        steps: steps[date] ?? [],
        floorsClimbed: floorsClimbed[date] ?? [],
        distanceWalkingRunning: distanceWalkingRunning[date] ?? [],
        vo2Max: vo2Max[date] ?? []
      )
    }

  let patch = ActivityPatch(activities: activities)
  return patch.isNotEmpty ? patch : nil
}

struct SleepGroupKey: Hashable {
  let bundleIdentifier: String
  let productType: String?

  var sourceType: SourceType {
    return .infer(sourceBundle: bundleIdentifier, productType: productType)
  }
}

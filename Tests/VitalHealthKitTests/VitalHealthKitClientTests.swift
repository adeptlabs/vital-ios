import XCTest
import HealthKit

@testable import VitalHealthKit

class VitalHealthKitClientTests: XCTestCase {
  
  func testSetupWithoutVitalClient() throws {
    /// This shouldn't crash if called before VitaClient.configure
    VitalHealthKitClient.configure(
      .init(
        backgroundDeliveryEnabled: true, logsEnabled: true
      )
    )
  }
}
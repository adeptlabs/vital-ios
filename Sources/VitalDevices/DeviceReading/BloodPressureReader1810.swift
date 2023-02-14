import VitalCore
import CombineCoreBluetooth

public protocol BloodPressureReadable: DevicePairable {
  func read(device: ScannedDevice) -> AnyPublisher<[BloodPressureSample], Error>
}

private let service = CBUUID(string: "1810")
private let BLE_BLOOD_PRESSURE_MEASURE_CHARACTERISTIC = "2A35"


class BloodPressureReader1810: BloodPressureReadable {
    
  private let manager: CentralManager
  private let queue: DispatchQueue
  
  init(manager: CentralManager = .live(), queue: DispatchQueue) {
    self.manager = manager
    self.queue = DispatchQueue(label: "io.tryvital.VitalDevices.BloodPressureReader1810", target: queue)
  }
  
  public func read(device: ScannedDevice) -> AnyPublisher<[BloodPressureSample], Error> {
    return _pair(device: device).flatMapLatest { (peripheral, characteristic) -> AnyPublisher<[BloodPressureSample], Error> in
      return peripheral.listenForUpdates(on: characteristic)
        .compactMap(toBloodPressureReading).eraseToAnyPublisher()
        .collect(.byTimeOrCount(self.queue, 3.0, 50))
        .eraseToAnyPublisher()
    }
  }
  
  public func pair(device: ScannedDevice) -> AnyPublisher<Void, Error> {
    _pair(device: device).map { _ in ()}.eraseToAnyPublisher()
  }
  
  private func _pair(device: ScannedDevice) -> AnyPublisher<(Peripheral, CBCharacteristic), Error> {
    let isOn: AnyPublisher<CBManagerState, Error> = manager
      .didUpdateState.filter { state in
        state == .poweredOn
      }
      .mapError { _ -> Error in }
      .eraseToAnyPublisher()
    
    if manager.state == .poweredOn {
      return BloodPressureReader1810._pair(manager: manager, device: device)
    } else {
      return isOn.flatMapLatest{[manager] _ in
        return BloodPressureReader1810._pair(manager: manager, device: device)
      }
    }
  }
  
  private static func _pair(manager: CentralManager, device: ScannedDevice) -> AnyPublisher<(Peripheral, CBCharacteristic), Error> {
    let service = DevicesManager.service(for: device.deviceModel.brand)
    let characteristic = CBUUID(string: BLE_BLOOD_PRESSURE_MEASURE_CHARACTERISTIC.fullUUID)

    return manager.connect(device.peripheral).flatMapLatest { peripheral -> AnyPublisher<(Peripheral, CBCharacteristic), Error> in
      
      peripheral.discoverServices([service])
        .flatMapLatest { services -> AnyPublisher<[CBCharacteristic], Error> in
          guard services.isEmpty == false else {
            return .empty
          }
          
          return peripheral.discoverCharacteristics([characteristic], for: services[0])
        }
        .flatMapLatest { characteristics -> AnyPublisher<(Peripheral, CBCharacteristic), Error> in
          guard characteristics.isEmpty == false else {
            return .empty
          }
          
          return peripheral.setNotifyValue(true, for: characteristics[0]).map { (peripheral, characteristics[0]) }.eraseToAnyPublisher()
        }
    }
    .eraseToAnyPublisher()
  }
}
  
private func toBloodPressureReading(data: Data?) -> BloodPressureSample? {
  guard let data = data else {
    return nil
  }
  
  let byteArrayFromData: [UInt8] = [UInt8](data)
  
  let units = (byteArrayFromData[0] & 1) != 0 ? "kPa" : "mmHg"
  
  let systolic: UInt16 = [byteArrayFromData[1], byteArrayFromData[2]].withUnsafeBytes { $0.load(as: UInt16.self) }
  let diastolic: UInt16 = [byteArrayFromData[3], byteArrayFromData[4]].withUnsafeBytes { $0.load(as: UInt16.self) }

  let year: UInt16 = [byteArrayFromData[7], byteArrayFromData[8]].withUnsafeBytes { $0.load(as: UInt16.self) }
  let month = byteArrayFromData[9]
  let day = byteArrayFromData[10]
  let hour = byteArrayFromData[11]
  let minute = byteArrayFromData[12]
  let second = byteArrayFromData[13]
  
  let components = DateComponents(year: Int(year), month: Int(month), day: Int(day), hour: Int(hour), minute: Int(minute), second: Int(second))
  let date = Calendar.current.date(from: components) ?? .init()
  
  let pulseRate: UInt16 = [byteArrayFromData[14], byteArrayFromData[15]].withUnsafeBytes { $0.load(as: UInt16.self) }
  
  let systolicSample = QuantitySample(value: Double(systolic), startDate: date, endDate: date, type: "cuff", unit: units)
  let diastolicSample = QuantitySample(value: Double(diastolic), startDate: date, endDate: date, type: "cuff", unit: units)
  let pulseSample = QuantitySample(value: Double(pulseRate), startDate: date, endDate: date, type: "cuff", unit: "bpm")
  
  return BloodPressureSample(
    systolic: systolicSample,
    diastolic: diastolicSample,
    pulse: pulseSample
  )
}

import Foundation
import CombineCoreBluetooth
import CoreBluetooth

public class DevicesManager {
  let manager: CentralManager
  
  public init() {
    self.manager = .live()
  }
  
  public func search(
    for deviceModel: DeviceModel
  ) -> AnyPublisher<ScannedDevice, Never> {
    
    let service = Self.service(for: deviceModel.brand)
    
    return manager
      .scanForPeripherals(withServices: [service])
      .compactMap { peripheralDiscover in
        guard let name = peripheralDiscover.peripheral.name else {
          return nil
        }
        
        let outcome = deviceModel.codes.reduce(false) { partialResult, code in
          guard partialResult == false else {
            return true
          }
          
          return name.contains(code)
        }
        
        guard outcome else {
          return nil
        }
        
        return peripheralDiscover.peripheral
      }
      .map { (peripheral: Peripheral) -> ScannedDevice in
        ScannedDevice(
          name: peripheral.name!,
          uuid: peripheral.id.uuidString,
          brand: deviceModel.brand,
          kind: deviceModel.kind,
          peripheral: peripheral
        )
      }
      .eraseToAnyPublisher()
  }
  
  public func monitorConnection(for device: ScannedDevice) -> AnyPublisher<Bool, Never> {
    manager.monitorConnection(for: device.peripheral).eraseToAnyPublisher()
  }
}

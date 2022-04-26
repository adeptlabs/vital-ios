import Foundation
import Get

public enum TimeSeriesResource {
  case glucose
}

public extension VitalNetworkClient {
  class TimeSeries {
    let client: VitalNetworkClient
    let resource = "timeseries"
    
    init(client: VitalNetworkClient) {
      self.client = client
    }
  }
  
  var timeSeries: TimeSeries {
    .init(client: self)
  }
}

public extension VitalNetworkClient.TimeSeries {
  func get(
    resource: TimeSeriesResource,
    provider: Provider? = nil,
    startDate: Date,
    endDate: Date? = nil
  ) async throws -> [TimeSeriesDataPoint] {
    
    guard let userId = self.client.userId else {
      fatalError("VitalNetwork's `userId` hasn't been set. Please call `setUserId`")
    }
    
    let query = makeQuery(startDate: startDate, endDate: endDate)
    
    switch resource {
      case .glucose:
        let path = makePath(for: "glucose", userId: userId.uuidString)

        let request: Request<[TimeSeriesDataPoint]> = .get(path, query: query, headers: [:])
        let response = try await self.client.apiClient.send(request)
        return response.value
    }
  }
  
  func getBloodPressure(
    provider: Provider? = nil,
    startDate: Date,
    endDate: Date? = nil
  ) async throws -> [BloodPressureDataPoint] {
    
    guard let userId = self.client.userId else {
      fatalError("VitalNetwork's `userId` hasn't been set. Please call `setUserId`")
    }
    
    let path = makePath(for: "blood_pressure", userId: userId.uuidString)
    let query = makeQuery(startDate: startDate, endDate: endDate)
    
    let request: Request<[BloodPressureDataPoint]> = .get(path, query: query, headers: [:])
    let response = try await self.client.apiClient.send(request)
    
    return response.value
  }
  
  
  func makePath(
    for resource: String,
    userId: String
  ) -> String {
    
    let prefix: String = "/\(client.apiVersion)"
      .append(self.resource)
      .append(userId)
    
    return prefix.append(resource)
  }
  
  func makeQuery(
    startDate: Date,
    endDate: Date?
  ) -> [(String, String?)] {
    
    let startDateString = self.client.dateFormatter.string(from: startDate)
    
    var query: [(String, String?)] = [("start_date", startDateString)]
    
    if let endDate = endDate {
      let endDateString = self.client.dateFormatter.string(from: endDate)
      query.append(("end_date", endDateString))
    }
    
    return query
  }
}

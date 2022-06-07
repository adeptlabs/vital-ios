import Get

public extension VitalClient {
  class User {
    let client: VitalClient
    let path = "user"
    
    init(client: VitalClient) {
      self.client = client
    }
  }
  
  var user: User {
    .init(client: self)
  }
}

public extension VitalClient.User {
  func create(
    _ payload: CreateUserRequest,
    setUserIdOnSuccess: Bool = true
  ) async throws -> CreateUserResponse {
    
    let path = "/\(self.client.apiVersion)/\(path)/"
    let request = Request<CreateUserResponse>.post(path, body: payload)
    
    self.client.logger?.info("Creating Vital's userId for id: \(payload.clientUserId)")
    let value = try await self.client.apiClient.send(request).value
    
    if setUserIdOnSuccess {
      VitalClient.setUserId(value.userId)
    }
    
    return value
  }
  
  func create(
    clientUserId: String,
    setUserIdOnSuccess: Bool = true
  ) async throws -> CreateUserResponse {
    return try await create(
      .init(clientUserId: clientUserId),
      setUserIdOnSuccess: setUserIdOnSuccess
    )
  }
}

import Foundation

public extension VitalClient {
  class Link {
    let client: VitalClient
    let path = "link"
    
    init(client: VitalClient) {
      self.client = client
    }
  }
  
  var link: Link {
    .init(client: self)
  }
}

public extension VitalClient.Link {
  
  func createConnectedSource(
    _ userId: UUID,
    provider: Provider
  ) async throws -> Void {
    
    let configuration = await self.client.configuration.get()

    let path = "/\(configuration.apiVersion)/\(path)/provider/manual/\(provider.rawValue)"
    
    let payload = CreateConnectionSourceRequest(userId: userId)
    let request = Request<Void>.post(path, body: payload)
    
    try await configuration.apiClient.send(request)
  }
  
  func createConnectedSource(
    for provider: Provider
  ) async throws -> Void {
    let userId = await self.client.userId.get()
    try await createConnectedSource(userId, provider: provider)
  }
  
  func createProviderLink(
    provider: Provider? = nil,
    redirectURL: String
  ) async throws -> URL {
    
    let userId = await self.client.userId.get()
    let configuration = await self.client.configuration.get()

    let path = "/\(configuration.apiVersion)/\(path)/token"
        
    let payload = CreateLinkRequest(userId: userId, provider: provider?.rawValue, redirectUrl: redirectURL)
    let request = Request<CreateLinkResponse>.post(path, body: payload)
    
    let response = try await configuration.apiClient.send(request)
    
    let url = URL(string: "https://link.tryvital.io/")!
      .append("token", value: response.value.linkToken)
      .append("env", value: configuration.environment.name)
      .append("region", value: configuration.environment.region.name)
      .append("isMobile", value: "True")
      
    return url
  }
}

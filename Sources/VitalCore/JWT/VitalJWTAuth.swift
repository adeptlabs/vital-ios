import Foundation

public enum VitalJWTSignInError: Error, Hashable {
  case alreadySignedIn
  case invalidSignInToken
}

public enum VitalJWTAuthError: Error, Hashable {
  /// There is no active SDK sign-in.
  case notSignedIn

  /// The user is no longer valid, and the SDK has been reset automatically.
  case invalidUser

  /// The refresh token is invalid, and the user must be signed in again with a new Vital Sign-In Token.
  case needsReauthentication
}

internal struct VitalJWTAuthUserContext {
  let userId: String
  let teamId: String
}

internal struct VitalJWTAuthNeedsRefresh: Error {}

internal actor VitalJWTAuth {
  internal static let live = VitalJWTAuth()
  private static let keychainKey = "vital_jwt_auth"

  nonisolated var currentUserId: String? {
    let record: VitalJWTAuthRecord? = try? storage.get(key: Self.keychainKey)
    return record?.userId
  }

  private let storage: VitalSecureStorage
  private let session: URLSession

  private let parkingLot = ParkingLot()
  private var cachedRecord: VitalJWTAuthRecord? = nil

  init(
    storage: VitalSecureStorage = VitalSecureStorage(keychain: .live)
  ) {
    self.storage = storage
    self.session = URLSession(configuration: .ephemeral)
  }

  func signIn(with signInToken: VitalSignInToken) async throws {
    let record = try getRecord()
    let claims = try signInToken.unverifiedClaims()

    if let record = record {
      if record.pendingReauthentication {
        // Check that we are reauthenticating the current user.
        if claims.teamId != record.teamId || claims.userId != record.userId || claims.environment != record.environment {
          throw VitalJWTSignInError.invalidSignInToken
        }
      } else {
        // No reauthentication request and already signed-in - Abort.
        throw VitalJWTSignInError.alreadySignedIn
      }
    }

    var components = URLComponents(string: "https://identitytoolkit.googleapis.com/v1/accounts:signInWithCustomToken")!
    components.queryItems = [URLQueryItem(name: "key", value: signInToken.publicKey)]
    var request = URLRequest(url: components.url!)

    request.httpMethod = "POST"
    request.setValue("application/json", forHTTPHeaderField: "Content-Type")
    request.httpBody = try JSONEncoder().encode(
      FirebaseTokenExchangeRequest(token: signInToken.userToken, tenantId: claims.gcipTenantId)
    )

    let (data, response) = try await session.data(for: request)
    let httpResponse = response as! HTTPURLResponse

    switch httpResponse.statusCode {
    case 200 ... 299:
      let decoder = JSONDecoder()
      let exchangeResponse = try decoder.decode(FirebaseTokenExchangeResponse.self, from: data)

      let record = VitalJWTAuthRecord(
        environment: claims.environment,
        userId: claims.userId,
        teamId: claims.teamId,
        gcipTenantId: claims.gcipTenantId,
        publicApiKey: signInToken.publicKey,
        accessToken: exchangeResponse.idToken,
        refreshToken: exchangeResponse.refreshToken,
        expiry: Date().addingTimeInterval(Double(exchangeResponse.expiresIn) ?? 3600)
     )

      try setRecord(record)

      VitalLogger.core.info("sign-in success; expiresIn = \(exchangeResponse.expiresIn, privacy: .public)")

    case 401:
      VitalLogger.core.info("sign-in failed (401)")
      throw VitalJWTSignInError.invalidSignInToken

    default:
      VitalLogger.core.info("sign-in failed (\(httpResponse.statusCode, privacy: .public)); data = \(String(data: data, encoding: .utf8) ?? "<nil>")")
      throw NetworkError(response: httpResponse, data: data)
    }
  }

  func signOut() async throws {
    try setRecord(nil)
  }

  func userContext() throws -> VitalJWTAuthUserContext {
    guard let record = try getRecord() else { throw VitalJWTAuthError.notSignedIn }
    return VitalJWTAuthUserContext(userId: record.userId, teamId: record.teamId)
  }

  /// If the action encounters 401 Unauthorized, throw `VitalJWTAuthNeedsRefresh` to initiate
  /// the token refresh flow.
  func withAccessToken<Result>(action: (String) async throws -> Result) async throws -> Result {
    /// When token refresh flow is ongoing, the ParkingLot is enabled and the call will suspend
    /// until the flow completes.
    /// Otherwise, the call will return immediately when the ParkingLot is disabled.
    try await parkingLot.parkIfNeeded()

    guard let record = try getRecord() else {
      throw VitalJWTAuthError.notSignedIn
    }

    do {
      guard !record.isExpired() else {
        throw VitalJWTAuthNeedsRefresh()
      }

      return try await action(record.accessToken)

    } catch is VitalJWTAuthNeedsRefresh {
      // Try to start refresh
      try await refreshToken()

      // Retry
      return try await withAccessToken(action: action)
    }
  }

  /// Start a token refresh flow, or wait for the started flow to complete.
  func refreshToken() async throws {
    try await withTaskCancellationHandler {
      try Task.checkCancellation()

      guard parkingLot.tryTo(.enable) else {
        // Another task has started the refresh flow.
        // Join the ParkingLot to wait for the token refresh completion.
        try await parkingLot.parkIfNeeded()
        return
      }

      defer { _ = parkingLot.tryTo(.disable) }

      guard let record = try getRecord() else {
        throw VitalJWTAuthError.notSignedIn
      }

      if record.pendingReauthentication {
        throw VitalJWTAuthError.needsReauthentication
      }

      var components = URLComponents(string: "https://securetoken.googleapis.com/v1/token")!
      components.queryItems = [URLQueryItem(name: "key", value: record.publicApiKey)]
      var request = URLRequest(url: components.url!)

      request.httpMethod = "POST"
      request.setValue("application/x-www-form-urlencoded", forHTTPHeaderField: "Content-Type")
      let encodedToken = record.refreshToken.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed)!
      request.httpBody = "grant_type=refresh_token&refresh_token=\(encodedToken)".data(using: .utf8)

      let (data, response) = try await session.data(for: request)
      let httpResponse = response as! HTTPURLResponse

      switch httpResponse.statusCode {
      case 200 ... 299:
        let decoder = JSONDecoder()
        let refreshResponse = try decoder.decode(FirebaseTokenRefreshResponse.self, from: data)

        var newRecord = record
        newRecord.refreshToken = refreshResponse.refreshToken
        newRecord.accessToken = refreshResponse.idToken
        newRecord.expiry = Date().addingTimeInterval(Double(refreshResponse.expiresIn) ?? 3600)

        try setRecord(newRecord)
        VitalLogger.core.info("refresh success; expiresIn = \(refreshResponse.expiresIn, privacy: .public)")

      default:
        if
          (400...499).contains(httpResponse.statusCode),
          let response = try? JSONDecoder().decode(FirebaseTokenRefreshError.self, from: data)
        {
          if response.isInvalidUser {
            try setRecord(nil)
            throw VitalJWTAuthError.invalidUser
          }

          if response.needsReauthentication {
            var record = record
            record.pendingReauthentication = true
            try setRecord(record)

            throw VitalJWTAuthError.needsReauthentication
          }
        }

        VitalLogger.core.info("refresh failed (\(httpResponse.statusCode, privacy: .public)); data = \(String(data: data, encoding: .utf8) ?? "<nil>")")
        throw NetworkError(response: httpResponse, data: data)
      }

    } onCancel: {
      _ = parkingLot.tryTo(.disable)
    }
  }

  private func getRecord() throws -> VitalJWTAuthRecord? {
    if let record = cachedRecord {
      return record
    }

    // Backfill from keychain
    do {
      let record: VitalJWTAuthRecord? = try storage.get(key: Self.keychainKey)
      self.cachedRecord = record
      return record
    } catch is DecodingError {
      VitalLogger.core.error("auto signout: failed to decode keychain auth record")
      try? setRecord(nil)
      return nil
    }
  }

  private func setRecord(_ record: VitalJWTAuthRecord?) throws {
    defer { self.cachedRecord = record }
    if let record = record {
      try storage.set(value: record, key: Self.keychainKey)
    } else {
      storage.clean(key: Self.keychainKey)
    }
  }
}

private struct VitalJWTAuthRecord: Codable {
  let environment: Environment
  let userId: String
  let teamId: String
  let gcipTenantId: String
  let publicApiKey: String
  var accessToken: String
  var refreshToken: String
  var expiry: Date
  var pendingReauthentication = false

  func isExpired(now: Date = Date()) -> Bool {
    expiry <= now
  }
}

private struct FirebaseTokenRefreshResponse: Decodable {
  let expiresIn: String
  let refreshToken: String
  let idToken: String

  enum CodingKeys: String, CodingKey {
    case expiresIn = "expires_in"
    case refreshToken = "refresh_token"
    case idToken = "id_token"
  }
}

private struct FirebaseTokenRefreshErrorResponse: Decodable {
  let error: FirebaseTokenRefreshError
}

private struct FirebaseTokenRefreshError: Decodable {
  let message: String
  let status: String

  var isInvalidUser: Bool {
    ["USER_DISABLED", "USER_NOT_FOUND"].contains(message)
  }

  var needsReauthentication: Bool {
    ["TOKEN_EXPIRED", "INVALID_REFRESH_TOKEN"].contains(message)
  }
}

private struct FirebaseTokenExchangeRequest: Encodable {
  let returnSecureToken = true
  let token: String
  let tenantId: String
}

private struct FirebaseTokenExchangeResponse: Decodable {
  let expiresIn: String
  let refreshToken: String
  let idToken: String
}

internal struct VitalSignInToken: Hashable, Decodable {
  let publicKey: String
  let userToken: String

  init(publicKey: String, userToken: String) {
    self.publicKey = publicKey
    self.userToken = userToken
  }

  enum CodingKeys: String, CodingKey {
    case publicKey = "public_key"
    case userToken = "user_token"
  }

  static func decode(from token: String) throws -> VitalSignInToken {
    guard let unwrappedData = Data(base64Encoded: token) else {
      throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "token is not valid base64 blob"))
    }
    return try JSONDecoder().decode(VitalSignInToken.self, from: unwrappedData)
  }

  func unverifiedClaims() throws -> VitalSignInTokenClaims {
    let components = userToken.split(separator: ".")
    guard components.count == 3 else {
      throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "malformed JWT [0]"))
    }

    guard
      let rawHeader = Data(base64Encoded: padBase64(components[0])),
      let rawClaims = Data(base64Encoded: padBase64(components[1]))
    else {
      throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "malformed JWT [1]"))
    }

    let decoder = JSONDecoder()
    let headers = try decoder.decode([String: String].self, from: rawHeader)

    guard headers["alg"] == "RS256" && headers["typ"] == "JWT" else {
      throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "malformed JWT [2]"))
    }

    return try decoder.decode(VitalSignInTokenClaims.self, from: rawClaims)
  }
}

private func padBase64(_ string: any StringProtocol) -> String {
  let pad = string.count % 4
  print(pad)
  print(string)
  if pad == 0 {
    return String(string)
  } else {
    return string + String(repeating: "=", count: 4 - pad)
  }
}

internal struct VitalSignInTokenClaims: Decodable {
  let userId: String
  let teamId: String
  let gcipTenantId: String
  let environment: Environment

  init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    self.userId = try container.decode(String.self, forKey: .userId)
    self.gcipTenantId = try container.decode(String.self, forKey: .gcipTenantId)
    let innerClaims = try container.decode(InnerClaims.self, forKey: .claims)
    self.teamId = innerClaims.vital_team_id

    let issuer = try container.decode(String.self, forKey: .issuer)

    // e.g. id-signer-{env}-{region}@vital-id-{env}-{region}.iam.gserviceaccount.com
    let matches = try NSRegularExpression(pattern: "^id-signer-([a-z]+)-([a-z]+)@vital-id-[a-z]+-[a-z]+.iam.gserviceaccount.com$")
    guard
      let result = matches.firstMatch(in: issuer, range: NSRange(issuer.startIndex ..< issuer.endIndex, in: issuer)),
      let environmentRange = Range(result.range(at: 1), in: issuer),
      let regionRange = Range(result.range(at: 2), in: issuer),
      let environment = Environment(
        environment: String(issuer[environmentRange]),
        region: String(issuer[regionRange])
      )
    else {
      throw DecodingError.dataCorrupted(.init(codingPath: [CodingKeys.issuer], debugDescription: "invalid issuer"))
    }

    self.environment = environment
  }

  private struct InnerClaims: Decodable {
    let vital_team_id: String
  }

  private enum CodingKeys: String, CodingKey {
    case issuer = "iss"
    case userId = "uid"
    case claims = "claims"
    case gcipTenantId = "tenant_id"
  }
}

/// A parking lot that holds waiting callers for token refresh flow to complete.
private final class ParkingLot: @unchecked Sendable {
  enum State: Sendable {
    case mustPark([UUID: CheckedContinuation<Void, Never>] = [:])
    case disabled
  }

  enum Action {
    case enable
    case disable
  }

  private var state: State = .disabled
  private var lock = NSLock()

  func tryTo(_ action: Action) -> Bool {
    let (callersToRelease, hasTransitioned): ([CheckedContinuation<Void, Never>], Bool) = lock.withLock {
      switch (self.state, action) {
      case (.disabled, .enable):
        self.state = .mustPark()
        return ([], true)

      case (let .mustPark(parked), .disable):
        self.state = .disabled
        return (Array(parked.values), true)

      case (.mustPark, .enable), (.disabled, .disable):
        return ([], false)
      }
    }

    if !callersToRelease.isEmpty {
      Task {
        await withTaskGroup(of: Void.self) { group in
          for continuation in callersToRelease {
            group.addTask { continuation.resume() }
          }
        }
      }
    }

    return hasTransitioned
  }

  func parkIfNeeded() async throws {
    let ticket = UUID()

    return try await withTaskCancellationHandler {
      try Task.checkCancellation()

      return await withCheckedContinuation { continuation in
        let mustPark = lock.withLock {
          switch self.state {
          case let .mustPark(parked):
            var parked = parked
            parked[ticket] = continuation
            self.state = .mustPark(parked)
            return true

          case .disabled:
            return false
          }
        }

        if mustPark == false {
          continuation.resume()
        }
      }

    } onCancel: {
      lock.withLock {
        switch self.state {
        case let .mustPark(parked):
          var parked = parked
          parked.removeValue(forKey: ticket)
          self.state = .mustPark(parked)
        case .disabled:
          break
        }
      }
    }
  }
}

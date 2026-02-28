# Charles Schwab Trader API (Individual) - Interface Contract

**Base URL:** `https://api.schwabapi.com/trader/v1`

---

## 1. Accounts

### Get list of account numbers
`GET /accounts/accountNumbers`

**Summary:** Get list of account numbers and their encrypted values. As the first step, consumers must invoke this service to retrieve the list of plain text/encrypted value pairs, and use encrypted account values for all subsequent calls for any `accountNumber` request.

**Parameters:** None

**Responses:**
- **200 OK**: List of valid "accounts".
  - **Headers**: `Schwab-Client-CorrelId` (Correlation Id)
  - **Body (JSON)**:
    ```json
    [
      {
        "accountNumber": "string",
        "hashValue": "string"
      }
    ]
    ```
- **Error Responses (400, 401, 403, 404, 500, 503)**: Returns an error object with `message` and `errors` array.

---

### Get linked accounts
`GET /accounts`

**Summary:** Get linked account(s) balances and positions for the logged in user.

**Parameters:**
- `fields` (query, optional): Determine which fields to return (e.g., `fields=positions`).

**Responses:**
- **200 OK**: List of valid accounts with balances and optionally positions.
  - **Body (JSON)**:
    ```json
    [
      {
        "securitiesAccount": {
          "accountNumber": "string",
          "roundTrips": 0,
          "isDayTrader": false,
          "positions": [...],
          "initialBalances": {...},
          "currentBalances": {...},
          "projectedBalances": {...}
        }
      }
    ]
    ```

---

### Get a specific account
`GET /accounts/{accountNumber}`

**Summary:** Get a specific account balance and positions for the logged in user.

**Parameters:**
- `accountNumber` (path, required): The encrypted ID of the account.
- `fields` (query, optional): Determine which fields to return (e.g., `fields=positions`).

**Responses:**
- **200 OK**: Specific account details.
  - **Body (JSON)**: Returns the same structure as `/accounts` but for a single account.

---

## 2. Orders

### Get all orders for a specific account
`GET /accounts/{accountNumber}/orders`

**Summary:** Get all orders for a specific account within a maximum range of 1 year.

**Parameters:**
- `accountNumber` (path, required): The encrypted ID of the account.
- `fromEnteredTime` (query, required): ISO-8601 format (e.g., `2024-03-29T00:00:00.000Z`).
- `toEnteredTime` (query, required): ISO-8601 format.
- `maxResults` (query, optional): Default is 3000.
- `status` (query, optional): Filter by order status.

**Responses:**
- **200 OK**: List of orders.
  - **Body (JSON)**: Array of order objects containing `orderId`, `status`, `price`, `orderLegCollection`, etc.

---

### Place order
`POST /accounts/{accountNumber}/orders`

**Summary:** Place an order for a specific account.

**Parameters:**
- `accountNumber` (path, required): The encrypted ID of the account.

**Request Body (JSON):**
```json
{
  "orderType": "MARKET",
  "session": "NORMAL",
  "duration": "DAY",
  "orderStrategyType": "SINGLE",
  "orderLegCollection": [
    {
      "instruction": "BUY",
      "quantity": 10,
      "instrument": {
        "symbol": "AAPL",
        "assetType": "EQUITY"
      }
    }
  ]
}
```

**Responses:**
- **201 Created**: Order successfully placed.
  - **Headers**: `Location` (Link to the newly created order).

---

### Get specific order
`GET /accounts/{accountNumber}/orders/{orderId}`

**Parameters:**
- `accountNumber` (path, required), `orderId` (path, required).

---

### Replace order
`PUT /accounts/{accountNumber}/orders/{orderId}`

**Summary:** Replace an existing order with new parameters.

---

### Cancel order
`DELETE /accounts/{accountNumber}/orders/{orderId}`

---

### Preview order
`POST /accounts/{accountNumber}/previewOrder`

**Summary:** Preview an order to see estimated commissions, fees, and validation results without placing it.

---

## 3. Transactions

### Get all transactions
`GET /accounts/{accountNumber}/transactions`

**Parameters:**
- `accountNumber` (path, required).
- `startDate` (query, required), `endDate` (query, required).
- `types` (query, required): Filter by transaction type (e.g., `TRADE`, `RECEIVE_AND_DELIVER`).

---

### Get specific transaction
`GET /accounts/{accountNumber}/transactions/{transactionId}`

---

## 4. User Preferences

### Get user preference
`GET /userPreference`

**Summary:** Get user preference information, including linked accounts, streamer info, and market data permissions.

**Responses:**
- **200 OK**:
  ```json
  [
    {
      "accounts": [...],
      "streamerInfo": [...],
      "offers": [...]
    }
  ]
  ```

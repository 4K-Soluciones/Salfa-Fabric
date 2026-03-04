# Seedz API Payloads Reference

## Base URL
- Sandbox: `https://api-sandbox.seedz.ag`
- Production: `https://api-prod.seedz.ag`

## Authentication
```
POST /v1/auth/login
Body: { "email": "...", "password": "..." }
Response: { "token": "eyJ..." }

Header for all subsequent calls:
  Authorization: Bearer {token}
  Content-Type: application/json
```

## Rate Limits
- Max 100 requests/minute per endpoint
- Batch size: recommended 500 records per POST
- Retry on 429 with exponential backoff (1s, 2s, 4s, 8s, max 60s)

---

## Superación Endpoints (8 entities)

### POST /v1/customer
```json
{
  "identification": "76123456-7_1010",
  "id": "12345",
  "createdAt": "15-03-2024 00:00:00",
  "updatedAt": "04-03-2026 06:00:00",
  "companyName": "EMPRESA ABC LTDA",
  "clientIdentification": "76987654-3",
  "phones": "56912345678",
  "email": "contacto@empresa.cl"
}
```

### POST /v1/property
```json
{
  "identification": "76123456-7_1010",
  "id": "12345",
  "createdAt": "15-03-2024 00:00:00",
  "updatedAt": "04-03-2026 06:00:00",
  "clientId": "12345",
  "companyName": "EMPRESA ABC LTDA",
  "identificationProperty": "76123456-7"
}
```

### POST /v1/item
```json
{
  "identification": "76123456-7_1010",
  "id": "RE12345",
  "createdAt": "01-01-2020 00:00:00",
  "updatedAt": "04-03-2026 06:00:00",
  "sku": "RE12345",
  "description": "FILTER OIL JD"
}
```

### POST /v1/seller
```json
{
  "identification": "V001",
  "id": "V001",
  "document": "12345678-9",
  "companyName": "JUAN PEREZ",
  "phones": "56987654321",
  "email": "jperez@salfa.cl",
  "createdAt": "01-06-2023 00:00:00",
  "updatedAt": "04-03-2026 06:00:00"
}
```

### POST /v1/order
```json
{
  "identification": "76123456-7_1010",
  "id": "5000001234",
  "createdAt": "10-02-2025 00:00:00",
  "updatedAt": "04-03-2026 06:00:00",
  "issuedAt": "10-02-2025",
  "status": "1",
  "clientId": "12345",
  "clientIdentification": "76987654-3",
  "companyIdentification": "76123456-7",
  "sellers": "V001"
}
```

### POST /v1/orderItem
```json
{
  "identification": "76123456-7_1010",
  "id": "5000001234_000010",
  "orderId": "5000001234",
  "issuedAt": "10-02-2025",
  "status": "1",
  "clientIdentification": "76987654-3",
  "idItem": "RE12345",
  "itemSku": "RE12345",
  "itemDescription": "FILTER OIL JD",
  "itemMeasureUnit": "UN",
  "quantity": "5",
  "netUnitValue": "12500.00",
  "totalValue": "62500.00",
  "createdAt": "10-02-2025 00:00:00",
  "updatedAt": "04-03-2026 06:00:00"
}
```

### POST /v1/invoice
```json
{
  "identification": "76123456-7_1010",
  "id": "9000001234",
  "clientId": "12345",
  "clientDocument": "76987654-3",
  "number": "9000001234",
  "serie": "1",
  "issuedAt": "15-02-2025",
  "type": "1",
  "status": "2",
  "statusId": "2",
  "orderId": "5000001234",
  "createdAt": "15-02-2025 00:00:00",
  "updatedAt": "04-03-2026 06:00:00"
}
```

### POST /v1/invoiceItem
```json
{
  "identification": "76123456-7_1010",
  "id": "9000001234_000010",
  "number": "9000001234",
  "serie": "1",
  "issuedAt": "15-02-2025",
  "orderId": "5000001234",
  "type": "1",
  "clientId": "12345",
  "clientDocument": "76987654-3",
  "parentInvoiceId": "",
  "parentInvoice": "",
  "itemId": "RE12345",
  "itemDescription": "FILTER OIL JD",
  "itemSku": "RE12345",
  "itemMeasureUnit": "UN",
  "itemQuantity": "5",
  "itemUnitValue": "12500.00",
  "itemTotalValue": "62500.00",
  "sellerIdentification": "V001",
  "category": "PARTS",
  "status": "Concluída",
  "statusId": "2",
  "itemCfop": "",
  "createdAt": "15-02-2025 00:00:00",
  "updatedAt": "04-03-2026 06:00:00"
}
```

### POST /v1/accountReceivable
```json
{
  "identification": "76123456-7_1010",
  "id": "1400000123_2025_001",
  "titlenumber": "1400000123",
  "clientId": "12345",
  "supplierName": "EMPRESA ABC LTDA",
  "issuedAt": "15-02-2025",
  "number": "1400000123",
  "serie": "2025",
  "originalDueAt": "15-03-2025",
  "extendedDueAt": "15-03-2025",
  "netValue": "62500.00",
  "status": "Aberto",
  "paidAt": "",
  "sellers": "",
  "createdAt": "15-02-2025 00:00:00",
  "updatedAt": "04-03-2026 06:00:00"
}
```

---

## Censacional Endpoint

### POST /v1/censacional
```json
{
  "dealerCode": "SALFA_CL",
  "billingDocument": "9000001234",
  "billingItem": "000010",
  "billingDate": "15-02-2025",
  "customerNumber": "12345",
  "customerName": "EMPRESA ABC LTDA",
  "materialNumber": "M7R2045T",
  "materialDescription": "TRACTOR 7R 2045 POWERTECH",
  "quantity": "1",
  "netValue": "125000000.00",
  "currency": "CLP",
  "plant": "1010",
  "chassi": "1RW7245TXKD012345",
  "model": "TRACTOR 7R 2045 POWERTECH",
  "vehicleState": "N",
  "sellerCode": "V001",
  "invoiceType": "1",
  "isMachinery": "true"
}
```

> **NOTE:** For PARTS (non-machinery): chassi="", model="", vehicleState="", isMachinery="false"

---

## Batch Format (all endpoints)

All POST calls accept arrays for batch sending:
```json
[
  { ...record1... },
  { ...record2... },
  ...
]
```
Recommended batch size: 500 records per call.

## Error Handling

| HTTP Code | Meaning | Action |
|---|---|---|
| 200 | Success | Log, continue |
| 400 | Bad request | Log payload, skip record, alert |
| 401 | Unauthorized | Re-authenticate, retry |
| 429 | Rate limited | Exponential backoff, retry |
| 500 | Server error | Retry 3x with backoff, then alert |

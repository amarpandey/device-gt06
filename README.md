# GT06 Device Server

This Node.js application accepts TCP connections from multiple Concox GT06 GPS tracker devices, parses all types of GT06 data packets, maps packets to device IMEI, stores parsed packets as JSON in MongoDB per device, converts packets to human-readable format, and sends correct protocol responses to devices.

## Features
- Accepts data from multiple GT06 devices over TCP
- Supports all GT06 packet types
- Maps packets to device IMEI
- Stores packets as JSON in MongoDB
- Parses and converts packets to human-readable format
- Sends protocol-compliant responses to devices

## Getting Started
1. Install dependencies:
   ```sh
   npm install
   ```
2. Set up your MongoDB connection string in a `.env` file:
   ```env
   MONGODB_URI=mongodb://localhost:27017/gt06
   ```
3. Start the server:
   ```sh
   node index.js
   ```

## Notes
- Make sure MongoDB is running and accessible.
- The server listens on a configurable port (default: 5023).
- Packet parsing and response logic is extensible for all GT06 packet types.

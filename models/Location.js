const mongoose = require('mongoose');

const locationSchema = new mongoose.Schema({
  // Device identification
  imei: { type: String, required: true, index: true },
  deviceId: String, // Device ID/Terminal ID from login packet
  deviceModel: String, // Device model identifier
  
  // GPS positioning data
  latitude: Number,
  longitude: Number,
  altitude: Number,
  speed: Number,
  course: Number,
  heading: Number,
  satellites: Number,
  hdop: Number, // Horizontal Dilution of Precision
  gpsFixed: Boolean,
  
  // LBS (Cell tower) positioning data
  mcc: Number, // Mobile Country Code
  mnc: Number, // Mobile Network Code
  lac: Number, // Location Area Code
  cellId: Number,
  gsmSignal: Number,
  
  // WiFi positioning (if available)
  wifiCount: Number,
  wifiData: String, // Serialized WiFi AP data
  
  // Timestamp
  timestamp: Date,
  timezoneOffset: Number, // Timezone offset in minutes
  
  // Packet metadata
  raw: String,
  packetType: String, // Human-readable packet type
  protocol: { type: String }, // Protocol number as hex, e.g., '0x12' or '0x22'
  deviceType: String,
  serialNumber: Number,
  
  // Status flags
  acc: Boolean, // ACC status (ignition)
  defense: Boolean, // Defense activated
  charge: Boolean, // Charging status
  gpsTracking: Boolean, // GPS tracking on/off
  oil: Boolean, // Oil/fuel cut status
  electric: Boolean, // Electric circuit status
  door: Boolean, // Door status (open/closed)
  
  // Alarm information
  alarm: String, // Alarm type if any
  alarmLanguage: String, // Alarm language setting
  
  // Vehicle/Device info
  mileage: Number, // Trip mileage in meters
  odometer: Number, // Total odometer reading
  voltage: Number, // External power voltage (V)
  batteryLevel: Number, // Internal battery percentage
  batteryVoltage: Number, // Battery voltage
  
  // Terminal/Device status
  terminalInfo: Number, // Terminal information byte
  languageIdentifier: Number, // Language setting
  fuelLevel: Number, // Fuel level percentage
  temperature: Number, // Temperature sensor reading
  
  // Additional status
  positioningType: String, // 'GPS', 'LBS', 'WIFI', 'COMBINED'
  realTime: Boolean, // Real-time GPS or buffered
}, { timestamps: true });

// Index for efficient querying
locationSchema.index({ imei: 1, timestamp: -1 });
locationSchema.index({ deviceId: 1 });
locationSchema.index({ packetType: 1 });

module.exports = mongoose.model('Location', locationSchema, 'gt06locations');

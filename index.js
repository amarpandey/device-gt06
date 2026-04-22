const mongoose = require('mongoose');
require('dotenv').config();
const net = require('net');
const Location = require('./models/Location');

const PORT = process.env.PORT || 5023;
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/di-stage';

// Mongoose setup
mongoose.connect(MONGODB_URI, { useNewUrlParser: true, useUnifiedTopology: true })
  .then(() => console.log('Connected to MongoDB with Mongoose'))
  .catch(err => console.error('MongoDB connection error:', err));

// Convert a BCD buffer to integer
function bcdToInt(bcd) {
  let result = 0;
  for (let i = 0; i < bcd.length; i++) {
    result = result * 100 + ((bcd[i] >> 4) * 10) + (bcd[i] & 0x0f);
  }
  return result;
}

// Parse GT06 packet with proper structure
function parseGT06Packet(buffer, deviceImei = null) {
  // Validate buffer
  if (!buffer || buffer.length < 5) {
    console.error('Invalid or too short buffer:', buffer?.toString('hex'));
    return {
      imei: deviceImei || 'unknown',
      protocolNumber: 0x00,
      data: null,
      raw: buffer?.toString('hex') || '',
      packetType: 'INVALID',
    };
  }
  
  // Check start bits
  const startBits = buffer.readUInt16BE(0);
  let isLongPacket = false;
  let packetLength = 0;
  let protocolNumber = 0;
  let dataStart = 0;
  
  if (startBits === 0x7878) {
    // Short packet format
    packetLength = buffer[2];
    protocolNumber = buffer[3];
    dataStart = 4;
  } else if (startBits === 0x7979) {
    // Long packet format
    isLongPacket = true;
    packetLength = buffer.readUInt16BE(2);
    protocolNumber = buffer[4];
    dataStart = 5;
  } else {
    console.error('Invalid start bits:', startBits.toString(16));
    return {
      imei: deviceImei || 'unknown',
      protocolNumber: 0x00,
      data: null,
      raw: buffer.toString('hex'),
      packetType: 'INVALID',
    };
  }
  
  // Get serial number (2 bytes before CRC)
  const serialNumber = buffer.readUInt16BE(buffer.length - 6);
  
  let data = {};
  let imei = deviceImei;
  let packetType = 'UNKNOWN';
  
  try {
    switch (protocolNumber) {
      case 0x01: // Login packet
        packetType = 'LOGIN';
        // IMEI is 8 bytes starting at dataStart
        const imeiBuffer = buffer.slice(dataStart, dataStart + 8);
        imei = imeiBuffer.toString('hex');
        data.imei = imei;
        
        // Device ID/Type identifier (2 bytes after IMEI)
        if (buffer.length >= dataStart + 10) {
          const deviceTypeId = buffer.readUInt16BE(dataStart + 8);
          data.deviceId = deviceTypeId.toString(16).toUpperCase();
          data.deviceModel = getDeviceModel(deviceTypeId);
        }
        break;
        
      case 0x12: // Location data
        packetType = 'LOCATION';
        // Date/time: 6 bytes (YY MM DD HH mm ss)
        const year12 = 2000 + buffer[dataStart];
        const month12 = buffer[dataStart + 1];
        const day12 = buffer[dataStart + 2];
        const hour12 = buffer[dataStart + 3];
        const minute12 = buffer[dataStart + 4];
        const second12 = buffer[dataStart + 5];
        data.timestamp = new Date(Date.UTC(year12, month12 - 1, day12, hour12, minute12, second12));
        
        // GPS satellites count (high 4 bits) and positioning length (low 4 bits)
        data.satellites = (buffer[dataStart + 6] >> 4) & 0x0F;
        
        // Latitude: 4 bytes
        const latRaw12 = buffer.readUInt32BE(dataStart + 7);
        data.latitude = latRaw12 / 1800000;

        // Longitude: 4 bytes
        const lngRaw12 = buffer.readUInt32BE(dataStart + 11);
        data.longitude = lngRaw12 / 1800000;
        
        // Speed: 1 byte
        data.speed = buffer[dataStart + 15];
        
        // Course/Status Info (2 bytes)
        // Bit 15: real-time GPS, Bit 14: GPS fixed, Bit 13: East(1)/West(0),
        // Bit 12: North(1)/South(0), Bit 11: speed valid, Bit 10: ACC on/off, Bits 0-9: course
        // NOTE: On this firmware only bits 14 (GPS fixed) and 13 (East/West) are inverted
        //       vs spec: 0=fixed, 0=East.  Bit 12 (North/South) is standard: 1=North.
        const courseStatus12 = buffer.readUInt16BE(dataStart + 16);
        data.course = courseStatus12 & 0x03FF; // Lower 10 bits
        data.realTime = (courseStatus12 & 0x8000) !== 0;
        data.gpsFixed = (courseStatus12 & 0x4000) === 0;  // Bit 14: 0=fixed on this device
        data.acc = (courseStatus12 & 0x0400) !== 0;        // Bit 10 = ACC/ignition
        const eastLng12 = (courseStatus12 & 0x2000) === 0; // Bit 13: 0=East on this device
        const northLat12 = (courseStatus12 & 0x1000) !== 0; // Bit 12: 1=North (standard, not inverted)
        if (!northLat12) data.latitude = -data.latitude;
        if (!eastLng12) data.longitude = -data.longitude;


        
        // MCC, MNC, LAC, Cell ID (if available)
        if (buffer.length > dataStart + 20) {
          data.mcc = buffer.readUInt16BE(dataStart + 18);
          data.mnc = buffer[dataStart + 20];
          data.lac = buffer.readUInt16BE(dataStart + 21);
          data.cellId = buffer.readUInt32BE(dataStart + 23) >> 8; // 3 bytes
        }
        break;
        
      case 0x13: // Status inquiry
        packetType = 'STATUS';
        // Parse status information
        if (buffer.length > dataStart + 3) {
          const terminalInfo = buffer[dataStart];
          data.terminalInfo = terminalInfo;
          
          // Parse terminal info byte - contains status flags
          const statusFlags = parseTerminalInfo(terminalInfo);
          data.oil = statusFlags.oil;
          data.electric = statusFlags.electric;
          data.door = statusFlags.door;
          data.acc = statusFlags.acc;
          data.defense = statusFlags.defense;
          data.gpsTracking = statusFlags.gpsTracking;
          
          // Voltage level (percentage)
          const voltageLevel = buffer[dataStart + 1];
          data.batteryLevel = voltageLevel;
          
          // GSM signal strength
          data.gsmSignal = buffer[dataStart + 2];
          
          // Alarm type
          if (buffer[dataStart + 3]) {
            data.alarm = getAlarmType(buffer[dataStart + 3]);
          }
          
          // Language identifier (if available)
          if (buffer.length > dataStart + 4) {
            data.languageIdentifier = buffer[dataStart + 4];
          }
        }
        data.timestamp = new Date();
        break;
        
      case 0x16: // Alarm data
        packetType = 'ALARM';
        // Similar to 0x12 but with alarm information
        const year16 = 2000 + buffer[dataStart];
        const month16 = buffer[dataStart + 1];
        const day16 = buffer[dataStart + 2];
        const hour16 = buffer[dataStart + 3];
        const minute16 = buffer[dataStart + 4];
        const second16 = buffer[dataStart + 5];
        data.timestamp = new Date(Date.UTC(year16, month16 - 1, day16, hour16, minute16, second16));
        
        data.satellites = (buffer[dataStart + 6] >> 4) & 0x0F;
        
        const latRaw16 = buffer.readUInt32BE(dataStart + 7);
        data.latitude = latRaw16 / 1800000;

        const lngRaw16 = buffer.readUInt32BE(dataStart + 11);
        data.longitude = lngRaw16 / 1800000;
        
        data.speed = buffer[dataStart + 15];
        
        const courseStatus16 = buffer.readUInt16BE(dataStart + 16);
        data.course = courseStatus16 & 0x03FF;
        data.realTime = (courseStatus16 & 0x8000) !== 0;
        data.gpsFixed = (courseStatus16 & 0x4000) === 0;   // Bit 14: 0=fixed on this device
        const eastLng16 = (courseStatus16 & 0x2000) === 0; // Bit 13: 0=East on this device
        const northLat16 = (courseStatus16 & 0x1000) !== 0; // Bit 12: 1=North (standard, not inverted)
        if (!northLat16) data.latitude = -data.latitude;
        if (!eastLng16) data.longitude = -data.longitude;
        
        // LBS info
        data.mcc = buffer.readUInt16BE(dataStart + 18);
        data.mnc = buffer[dataStart + 20];
        data.lac = buffer.readUInt16BE(dataStart + 21);
        data.cellId = buffer.readUInt32BE(dataStart + 23) >> 8;
        
        // Terminal information and alarm
        if (buffer.length > dataStart + 26) {
          const terminalInfo16 = buffer[dataStart + 26];
          data.terminalInfo = terminalInfo16;
          const statusFlags16 = parseTerminalInfo(terminalInfo16);
          data.oil = statusFlags16.oil;
          data.electric = statusFlags16.electric;
          data.door = statusFlags16.door;
          data.defense = statusFlags16.defense;
          data.gpsTracking = statusFlags16.gpsTracking;
          data.acc = statusFlags16.acc;

          // Alarm language/type
          if (buffer.length > dataStart + 27) {
            data.alarm = getAlarmType(buffer[dataStart + 27]);
          }
        }
        break;
        
      case 0x22: // Location data (extended)
        packetType = 'LOCATION_EXT';
        // Date/time: 6 bytes
        const year22 = 2000 + buffer[dataStart];
        const month22 = buffer[dataStart + 1];
        const day22 = buffer[dataStart + 2];
        const hour22 = buffer[dataStart + 3];
        const minute22 = buffer[dataStart + 4];
        const second22 = buffer[dataStart + 5];
        data.timestamp = new Date(Date.UTC(year22, month22 - 1, day22, hour22, minute22, second22));
        
        // Satellites (high nibble) and GSM signal (low nibble)
        data.satellites = (buffer[dataStart + 6] >> 4) & 0x0F;
        data.gsmSignal = buffer[dataStart + 6] & 0x0F;
        
        // Latitude: 4 bytes
        const latRaw22 = buffer.readUInt32BE(dataStart + 7);
        data.latitude = latRaw22 / 1800000;

        // Longitude: 4 bytes
        const lngRaw22 = buffer.readUInt32BE(dataStart + 11);
        data.longitude = lngRaw22 / 1800000;
        
        // Speed: 1 byte
        data.speed = buffer[dataStart + 15];
        
        // Course/Status (2 bytes)
        // Bit 15: real-time, Bit 14: GPS fixed, Bit 13: East/West, Bit 12: North/South,
        // Bit 11: speed valid, Bit 10: ACC, Bits 0-9: course
        // NOTE: On this firmware only bits 14 (GPS fixed) and 13 (East/West) are inverted
        //       vs spec: 0=fixed, 0=East.  Bit 12 (North/South) is standard: 1=North.
        const courseStatus22 = buffer.readUInt16BE(dataStart + 16);
        data.course = courseStatus22 & 0x03FF;
        data.realTime = (courseStatus22 & 0x8000) !== 0;
        data.gpsFixed = (courseStatus22 & 0x4000) === 0;   // Bit 14: 0=fixed on this device
        data.acc = (courseStatus22 & 0x0400) !== 0;         // Bit 10 = ACC/ignition
        const eastLng22 = (courseStatus22 & 0x2000) === 0; // Bit 13: 0=East on this device
        const northLat22 = (courseStatus22 & 0x1000) !== 0; // Bit 12: 1=North (standard, not inverted)
        if (!northLat22) data.latitude = -data.latitude;
        if (!eastLng22) data.longitude = -data.longitude;
        
        // MCC, MNC, LAC, Cell ID
        data.mcc = buffer.readUInt16BE(dataStart + 18);
        data.mnc = buffer[dataStart + 20];
        data.lac = buffer.readUInt16BE(dataStart + 21);
        data.cellId = buffer.readUInt32BE(dataStart + 23) >> 8;
        break;
        
      case 0x17: // GPS address request
        packetType = 'GPS_ADDRESS_REQUEST';
        // Parse GPS address packet
        if (buffer.length > dataStart + 17) {
          const year17 = 2000 + buffer[dataStart];
          const month17 = buffer[dataStart + 1];
          const day17 = buffer[dataStart + 2];
          const hour17 = buffer[dataStart + 3];
          const minute17 = buffer[dataStart + 4];
          const second17 = buffer[dataStart + 5];
          data.timestamp = new Date(Date.UTC(year17, month17 - 1, day17, hour17, minute17, second17));
          
          data.satellites = (buffer[dataStart + 6] >> 4) & 0x0F;
          const latRaw17 = buffer.readUInt32BE(dataStart + 7);
          data.latitude = latRaw17 / 1800000;
          const lngRaw17 = buffer.readUInt32BE(dataStart + 11);
          data.longitude = lngRaw17 / 1800000;
          data.speed = buffer[dataStart + 15];
          const courseStatus17 = buffer.readUInt16BE(dataStart + 16);
          data.course = courseStatus17 & 0x03FF;
          data.gpsFixed = (courseStatus17 & 0x4000) === 0;   // Bit 14: 0=fixed on this device
          const eastLng17 = (courseStatus17 & 0x2000) === 0; // Bit 13: 0=East on this device
          const northLat17 = (courseStatus17 & 0x1000) !== 0; // Bit 12: 1=North (standard, not inverted)
          if (!northLat17) data.latitude = -data.latitude;
          if (!eastLng17) data.longitude = -data.longitude;
        }
        break;
        
      case 0x1A: // GPS and LBS combined positioning
        packetType = 'COMBINED_POSITIONING';
        data.positioningType = 'COMBINED';
        // Similar structure to 0x22
        if (buffer.length > dataStart + 17) {
          const year1A = 2000 + buffer[dataStart];
          const month1A = buffer[dataStart + 1];
          const day1A = buffer[dataStart + 2];
          const hour1A = buffer[dataStart + 3];
          const minute1A = buffer[dataStart + 4];
          const second1A = buffer[dataStart + 5];
          data.timestamp = new Date(Date.UTC(year1A, month1A - 1, day1A, hour1A, minute1A, second1A));
          
          data.satellites = (buffer[dataStart + 6] >> 4) & 0x0F;
          data.gsmSignal = buffer[dataStart + 6] & 0x0F;
          
          const latRaw1A = buffer.readUInt32BE(dataStart + 7);
          data.latitude = latRaw1A / 1800000;
          const lngRaw1A = buffer.readUInt32BE(dataStart + 11);
          data.longitude = lngRaw1A / 1800000;
          data.speed = buffer[dataStart + 15];
          
          const courseStatus1A = buffer.readUInt16BE(dataStart + 16);
          data.course = courseStatus1A & 0x03FF;
          data.realTime = (courseStatus1A & 0x8000) !== 0;
          data.gpsFixed = (courseStatus1A & 0x4000) === 0;   // Bit 14: 0=fixed on this device
          data.acc = (courseStatus1A & 0x0400) !== 0;         // Bit 10 = ACC/ignition
          const eastLng1A = (courseStatus1A & 0x2000) === 0; // Bit 13: 0=East on this device
          const northLat1A = (courseStatus1A & 0x1000) !== 0; // Bit 12: 1=North (standard, not inverted)
          if (!northLat1A) data.latitude = -data.latitude;
          if (!eastLng1A) data.longitude = -data.longitude;
          
          // LBS data
          if (buffer.length > dataStart + 25) {
            data.mcc = buffer.readUInt16BE(dataStart + 18);
            data.mnc = buffer[dataStart + 20];
            data.lac = buffer.readUInt16BE(dataStart + 21);
            data.cellId = buffer.readUInt32BE(dataStart + 23) >> 8;
          }
        }
        break;
        
      case 0x23: // Heartbeat packet
        packetType = 'HEARTBEAT';
        // Heartbeat contains terminal information
        if (buffer.length > dataStart) {
          data.terminalInfo = buffer[dataStart];
          data.voltage = buffer[dataStart + 1] || null;
          data.gsmSignal = buffer[dataStart + 2] || null;
          data.languageIdentifier = buffer[dataStart + 3] || null;
        }
        data.timestamp = new Date();
        break;
        
      case 0x26: // Online command response
        packetType = 'ONLINE_COMMAND';
        // Server command response
        if (buffer.length > dataStart) {
          data.commandType = buffer[dataStart];
          data.commandLength = buffer.readUInt16BE(dataStart + 1);
        }
        data.timestamp = new Date();
        break;
        
      case 0x2A: // Time zone and language setting
        packetType = 'TIMEZONE_LANGUAGE';
        if (buffer.length > dataStart + 1) {
          data.languageIdentifier = buffer[dataStart];
          data.timezoneOffset = buffer[dataStart + 1]; // Offset in half hours
        }
        data.timestamp = new Date();
        break;
        
      case 0x80: // Command information
        packetType = 'COMMAND_INFO';
        // Contains command responses
        data.timestamp = new Date();
        break;
        
      case 0x94: // Information transmission
        packetType = 'INFO_TRANSMISSION';
        // Parse information type
        if (buffer.length > dataStart) {
          const infoType = buffer[dataStart];
          data.infoType = infoType;
          
          // Different info types
          if (infoType === 0x01) {
            // Mileage (odometer)
            if (buffer.length >= dataStart + 5) {
              data.odometer = buffer.readUInt32BE(dataStart + 1);
            }
          } else if (infoType === 0x05) {
            // External power voltage
            if (buffer.length >= dataStart + 3) {
              data.voltage = buffer.readUInt16BE(dataStart + 1) / 100; // In volts
            }
          }
        }
        data.timestamp = new Date();
        break;
        
      case 0x98: // Service extension
        packetType = 'SERVICE_EXTENSION';
        data.timestamp = new Date();
        break;
        
      default:
        packetType = `UNKNOWN_0x${protocolNumber.toString(16).toUpperCase()}`;
        data.timestamp = new Date();
        break;
    }
  } catch (error) {
    console.error('Error parsing packet:', error.message);
  }
  
  return {
    imei: imei || 'unknown',
    protocolNumber,
    packetType,
    data,
    serialNumber,
    raw: buffer.toString('hex'),
  };
}

// Parse terminal information byte to extract status flags
function parseTerminalInfo(terminalByte) {
  return {
    oil: (terminalByte & 0x01) === 0,        // Bit 0: 0=oil on, 1=oil off
    electric: (terminalByte & 0x02) === 0,   // Bit 1: 0=electric on, 1=electric off
    door: (terminalByte & 0x04) !== 0,       // Bit 2: 0=door closed, 1=door open
    acc: (terminalByte & 0x08) !== 0,        // Bit 3: 0=ACC off, 1=ACC on (ignition)
    defense: (terminalByte & 0x10) !== 0,    // Bit 4: 0=defense off, 1=defense on
    gpsTracking: (terminalByte & 0x20) !== 0 // Bit 5: 0=GPS tracking off, 1=on
  };
}

// Get alarm type description
function getAlarmType(alarmByte) {
  const alarmTypes = {
    0x00: 'NORMAL',
    0x01: 'SOS',
    0x02: 'POWER_CUT',
    0x03: 'VIBRATION',
    0x04: 'ENTER_FENCE',
    0x05: 'EXIT_FENCE',
    0x06: 'OVERSPEED',
    0x07: 'MOVEMENT',
    0x08: 'ENTER_GPS_DEADZONE',
    0x09: 'EXIT_GPS_DEADZONE',
    0x0A: 'POWER_ON',
    0x0B: 'LOW_BATTERY',
    0x0C: 'LOW_EXTERNAL_POWER',
    0x0D: 'GPS_ANTENNA_CUT',
    0x0E: 'GPS_ANTENNA_SHORT',
    0x0F: 'SIM_CHANGE',
  };
  return alarmTypes[alarmByte] || `UNKNOWN_${alarmByte}`;
}

// Get device model from device type ID
function getDeviceModel(typeId) {
  const models = {
    0x0001: 'GT06',
    0x0002: 'GT06N',
    0x0003: 'GT02',
    0x0004: 'GT06E',
    0x0005: 'GT100',
  };
  return models[typeId] || `GT06_${typeId.toString(16).toUpperCase()}`;
}



// Protocol response
function getResponseForPacket(packet, buffer) {
  const serialNumber = packet.serialNumber || 0x0001;
  const serialBuffer = Buffer.alloc(2);
  serialBuffer.writeUInt16BE(serialNumber, 0);
  
  // Login packet: protocol number 0x01
  if (packet.protocolNumber === 0x01) {
    // Build response: 78 78 05 01 [serial] [crc] 0D 0A
    const response = Buffer.alloc(10);
    response.write('7878', 0, 'hex');
    response.writeUInt8(0x05, 2); // Length
    response.writeUInt8(0x01, 3); // Protocol
    serialBuffer.copy(response, 4);
    const crc = crc16(response.slice(2, 6));
    response.writeUInt16BE(crc, 6);
    response.write('0d0a', 8, 'hex');
    return response;
  }
  
  // For location and other packets, send acknowledgment with same serial number
  // Build response: 78 78 05 [protocol] [serial] [crc] 0D 0A
  const response = Buffer.alloc(10);
  response.write('7878', 0, 'hex');
  response.writeUInt8(0x05, 2); // Length
  response.writeUInt8(packet.protocolNumber, 3); // Echo protocol number
  serialBuffer.copy(response, 4);
  const crc = crc16(response.slice(2, 6));
  response.writeUInt16BE(crc, 6);
  response.write('0d0a', 8, 'hex');
  return response;
}

// CRC-16-CCITT calculation for GT06
function crc16(buf) {
  let crc = 0xFFFF;
  for (let i = 0; i < buf.length; i++) {
    crc ^= buf[i] << 8;
    for (let j = 0; j < 8; j++) {
      if ((crc & 0x8000) !== 0) {
        crc = (crc << 1) ^ 0x1021;
      } else {
        crc = crc << 1;
      }
      crc &= 0xFFFF;
    }
  }
  return crc;
}

// Store device IMEI per socket connection
const deviceSessions = new Map();

const server = net.createServer(socket => {
  const socketId = `${socket.remoteAddress}:${socket.remotePort}`;
  let deviceImei = null;
  let deviceId = null;
  let deviceModel = null;
  
  socket.on('data', async data => {
    try {
      console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
      console.log('Received data:', data.toString('hex'), 'Length:', data.length);
      
      // Parse packet with stored IMEI
      const packet = parseGT06Packet(data, deviceImei);

      // If this is a login packet, store the IMEI and device ID for this connection
      if (packet.protocolNumber === 0x01 && packet.data.imei) {
        deviceImei = packet.data.imei;
        deviceId = packet.data.deviceId;
        deviceModel = packet.data.deviceModel;
        deviceSessions.set(socketId, { imei: deviceImei, deviceId, deviceModel });
        console.log('✓ Device logged in - IMEI:', deviceImei, 'DeviceID:', deviceId, 'Model:', deviceModel);
      }
      
      // Log the packet info
      console.table([{ 
        'Packet Type': packet.packetType,
        'Protocol': '0x' + packet.protocolNumber.toString(16).toUpperCase(), 
        'IMEI': packet.imei,
        'Device ID': deviceId || 'N/A',
        'Serial': packet.serialNumber
      }]);
      
      // Save ALL packet data to MongoDB (except invalid packets)
      if (packet.imei !== 'unknown' && packet.protocolNumber !== 0x00 && packet.data) {
        const locationDoc = {
          // Device identification
          imei: packet.imei,
          deviceId: deviceId || packet.data.deviceId,
          deviceModel: deviceModel || packet.data.deviceModel,
          deviceType: 'GT06',
          
          // GPS positioning data
          latitude: packet.data.latitude,
          longitude: packet.data.longitude,
          altitude: packet.data.altitude,
          speed: packet.data.speed,
          course: packet.data.course,
          heading: packet.data.heading,
          satellites: packet.data.satellites,
          hdop: packet.data.hdop,
          gpsFixed: packet.data.gpsFixed,
          
          // LBS (Cell tower) positioning data
          mcc: packet.data.mcc,
          mnc: packet.data.mnc,
          lac: packet.data.lac,
          cellId: packet.data.cellId,
          gsmSignal: packet.data.gsmSignal,
          
          // WiFi positioning
          wifiCount: packet.data.wifiCount,
          wifiData: packet.data.wifiData,
          
          // Timestamp
          timestamp: packet.data.timestamp || new Date(),
          timezoneOffset: packet.data.timezoneOffset,
          
          // Packet metadata
          raw: packet.raw,
          packetType: packet.packetType,
          protocol: '0x' + packet.protocolNumber.toString(16).toUpperCase(),
          serialNumber: packet.serialNumber,
          
          // Status flags
          acc: packet.data.acc,
          defense: packet.data.defense,
          charge: packet.data.charge,
          gpsTracking: packet.data.gpsTracking,
          oil: packet.data.oil,
          electric: packet.data.electric,
          door: packet.data.door,
          
          // Alarm information
          alarm: packet.data.alarm,
          alarmLanguage: packet.data.alarmLanguage,
          
          // Vehicle/Device info
          mileage: packet.data.mileage,
          odometer: packet.data.odometer,
          voltage: packet.data.voltage,
          batteryLevel: packet.data.batteryLevel,
          batteryVoltage: packet.data.batteryVoltage,
          
          // Terminal/Device status
          terminalInfo: packet.data.terminalInfo,
          languageIdentifier: packet.data.languageIdentifier,
          fuelLevel: packet.data.fuelLevel,
          temperature: packet.data.temperature,
          
          // Additional status
          positioningType: packet.data.positioningType || (packet.data.gpsFixed ? 'GPS' : packet.data.cellId ? 'LBS' : 'UNKNOWN'),
          realTime: packet.data.realTime,
        };
        
        // If this packet has GPS coordinates but GPS is not fixed, clear them —
        // unfixed positions have unreliable E/W flags and garbage coordinates.
        // Non-GPS packets (STATUS, HEARTBEAT) have no latitude at all so this is safe.
        if (locationDoc.latitude !== undefined && !packet.data.gpsFixed) {
          console.log(`⚠ GPS not fixed for ${packet.packetType} — coordinates discarded (lat=${locationDoc.latitude}, lng=${locationDoc.longitude})`);
          delete locationDoc.latitude;
          delete locationDoc.longitude;
          delete locationDoc.speed;
          delete locationDoc.course;
          delete locationDoc.satellites;
        }

        // Remove undefined fields to keep the document clean
        Object.keys(locationDoc).forEach(key =>
          locationDoc[key] === undefined && delete locationDoc[key]
        );

        await saveWithRetry(() => Location.create(locationDoc));
        console.log(`✓ ${packet.packetType} packet saved to MongoDB for IMEI: ${packet.imei}`);
      }
      
      // Send response
      const response = getResponseForPacket(packet, data);
      socket.write(response);
      console.log('✓ Response sent to device');
      console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');
      
    } catch (error) {
      console.error('✗ Error processing packet:', error.message);
      console.error('Stack:', error.stack);
    }
  });
  
  socket.on('close', () => {
    console.log('✗ Connection closed for', socketId);
    deviceSessions.delete(socketId);
  });
  
  socket.on('error', err => console.error('✗ Socket error:', err));
});

// Transient topology errors (primary failover, stale routing table) surface
// as a one-shot error that the driver resolves on the next heartbeat. A short
// retry loop keeps these packets from being dropped.
const RETRYABLE_CODES = new Set([10107, 13435, 13436, 189, 91, 11600, 11602]);

function isRetryable(err) {
  if (!err) return false;
  if (typeof err.hasErrorLabel === 'function' && err.hasErrorLabel('RetryableWriteError')) return true;
  if (err.code && RETRYABLE_CODES.has(err.code)) return true;
  const msg = (err.message || '').toLowerCase();
  return msg.includes('marked stale')
      || msg.includes('not primary')
      || msg.includes('not writable primary')
      || msg.includes('no primary');
}

async function saveWithRetry(fn, attempts = 3) {
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (!isRetryable(err) || i === attempts - 1) throw err;
      await new Promise(r => setTimeout(r, 200 * (i + 1))); // 200ms, 400ms
    }
  }
  throw lastErr;
}

server.listen(PORT, () => {
  console.log(`GT06 server listening on port ${PORT}`);
});

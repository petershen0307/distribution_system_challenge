package snowflakeid

import "time"

const timestampBits = 41
const machineIDBits = 10
const serialIDBits = 12
const MaximumSerialID = 2 << serialIDBits

// 41 bits for timestamp, 10 bits for machine ID, 12 bits for serial ID
const timestampMask = uint64(0x0000_03FF_FFFF_FFFE)
const machineIDMask = uint64(0x000F_FC00_0000_0000)
const serialIDMask = uint64(0xFFF0_0000_0000_0000)

func GenerateSnowflakeID(timestamp time.Time, machineID, machineSerialID int) uint64 {
	// https://en.wikipedia.org/wiki/Snowflake_ID

	return (uint64(timestamp.UTC().UnixMilli()) << 1 & timestampMask) | ((uint64(machineID) << (1 + timestampBits)) & machineIDMask) | ((uint64(machineSerialID) << (1 + timestampBits + machineIDBits)) & serialIDMask)
}

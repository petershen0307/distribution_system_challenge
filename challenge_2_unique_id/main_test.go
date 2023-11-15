package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_snowflake_gen(t *testing.T) {
	// arrange
	timestamp := time.Now()
	machineID := 1
	serialID := 1
	// act
	snowflake := generateSnowflakeID(timestamp, machineID, serialID)
	// assert
	assert.Equal(t, uint64(timestamp.UTC().UnixMilli()<<1), snowflake&timestampMask)
	assert.Equal(t, uint64(machineID)<<(1+timestampBits), snowflake&machineIDMask)
	assert.Equal(t, uint64(machineID)<<(1+timestampBits+machineIDBits), snowflake&serialIDMask)
}

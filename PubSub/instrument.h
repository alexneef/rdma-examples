#ifndef _instrument
#define _instrument

typedef struct instrument
{
	char Symbol[5];
	double 	Value;
	char Pad[250];
} instrument_t;

typedef struct MemoryRegionInfo
{
	void* addr;
	uint32_t rkey;
} MemoryRegionInfo_t;

#endif //_instrument

#include<stdio.h>
#include<stdlib.h>
#include<libvirt/libvirt.h>
#include<math.h>
#include<string.h>
#include<unistd.h>
#include<limits.h>
#include<signal.h>
#define MIN(a,b) ((a)<(b)?a:b)
#define MAX(a,b) ((a)>(b)?a:b)
#define NO_ERROR 0

#define TO_STRING(x) STRINGIFY(a)
#define STRINGIFY(a) #a

#define MINIMUM_HOST_MEMORY   2000

#define DOMAIN_MIN_FREE_RAM    100
#define DOMAIN_MAX_FREE_RAM    500

#define DOMAIN_MIN_TOTAL_RAM    200
#define DOMAIN_MAX_TOTAL_RAM   2000

#define DOMAIN_MEMORY_INCREMENT     50

int is_exit = 0; // DO NOT MODIFY THE VARIABLE

void MemoryScheduler(virConnectPtr conn,int interval);

typedef struct vCPUInfo {
    int                domainNumber;
    virDomainPtr       domain;
    virVcpuInfoPtr     domainInfo;
    const char *       domainName;
    long long          relativeLoad;

    struct vCPUInfo * lowerLoad;
    struct vCPUInfo * higherLoad;
		virDomainMemoryStatPtr stats;

    unsigned long long  unused;
    unsigned long long  balloon;
    unsigned long long  usable;
    unsigned long long  total;

    int needsMoreRAM;
} vCPUInfo;

unsigned long long hostTotalMemory = 0;
unsigned long long hostFreeMemory = 0;
unsigned long long totalAllVMs = 0;

virNodeMemoryStatsPtr params = NULL;
int numberOfParams = 0;

vCPUInfo *vcpus = NULL;
int numDomains = 0;


int RefreshDomainMemoryStats(vCPUInfo * vcpu)
{
		int numberOfStats = virDomainMemoryStats(vcpu->domain, vcpu->stats, VIR_DOMAIN_MEMORY_STAT_NR, 0);
		if (0 >= numberOfStats)
		{
			  return -1;
		}

		printf("  - Number of stats for %s: %d\n", vcpu->domainName, numberOfStats);

		for (int i = 0; i < numberOfStats; i++)
		{
				int statID = vcpu->stats[i].tag;
        unsigned long long value = vcpu->stats[i].val;

				if (VIR_DOMAIN_MEMORY_STAT_SWAP_IN == statID)
				{
				}
        else if (VIR_DOMAIN_MEMORY_STAT_SWAP_OUT == statID)
        {
        }
        else if (VIR_DOMAIN_MEMORY_STAT_MAJOR_FAULT == statID)
        {
        }
        else if (VIR_DOMAIN_MEMORY_STAT_MINOR_FAULT == statID)
        {
        }
        else if (VIR_DOMAIN_MEMORY_STAT_UNUSED == statID)
        {
            printf("    -> [unused] - %lluMB\n", value/1024);
            vcpu->unused = value/1024;
        }
        else if (VIR_DOMAIN_MEMORY_STAT_AVAILABLE == statID)
        {
            printf("    -> [total] - %lluMB\n", value/1024);
            vcpu->total = value/1024;
            totalAllVMs += vcpu->total;
        }
        else if (VIR_DOMAIN_MEMORY_STAT_ACTUAL_BALLOON == statID)
        {
            printf("    -> [balloon] - %lluMB\n", value/1024);
            vcpu->balloon = value/1024;
        }
        else if (VIR_DOMAIN_MEMORY_STAT_RSS == statID)
        {
        }
        else if (VIR_DOMAIN_MEMORY_STAT_USABLE == statID)
        {
            printf("    -> [usable] - %lluMB\n", value/1024);
        }
        else if (VIR_DOMAIN_MEMORY_STAT_LAST_UPDATE == statID)
        {
            printf("    -> [time] - %llu\n", value);
        }
		}

		return NO_ERROR;
}


int RefreshHostMemoryStats(virConnectPtr conn)
{
    int rva = virNodeGetMemoryStats(conn, VIR_NODE_MEMORY_STATS_ALL_CELLS, params, &numberOfParams, 0);
    if (rva < 0)
    {
        printf("Could not refresh hot memory statistics, got %d!\n", rva);
        return -1;
    }


    for (int i = 0; i < numberOfParams; i++)
    {
        if (strcmp(params[i].field, "free") == 0)
        {
            hostFreeMemory = params[i].value/1024;
        }
        else if (strcmp(params[i].field, "total" ) == 0)
        {
            hostTotalMemory = params[i].value/1024;
        }
    }

    return NO_ERROR;
}


int UpdateDomainMemoryAllocation(virConnectPtr conn, vCPUInfo * vcpu)
{
    unsigned long long memory = vcpu->balloon;
    unsigned long long unused = vcpu->unused;

    // Always refresh host memory stats before each adjustment.
    // The last VCPU update could have consumed some, so make sure
    //  we have enough before offing it to guest

    RefreshHostMemoryStats(conn);
    int vmsNeedFreeRAM = 0;

    for(int i = 0; i < numDomains; i++)
    {
        vcpus[i].needsMoreRAM = 0;
        vmsNeedFreeRAM += vcpus[i].needsMoreRAM;
    }
    printf("  - %d need more RAM\n", vmsNeedFreeRAM);

    // Case 1 -  We need more RAM for this VM
    if (unused < DOMAIN_MIN_FREE_RAM || memory < DOMAIN_MIN_TOTAL_RAM)
    {
        printf("  !! %s has too little memory!\n", vcpu->domainName);

        // Case 1.1 -  We need more RAM for this VM, but any increase would put
        //             us over the OS's maximum
        if (memory + DOMAIN_MEMORY_INCREMENT > DOMAIN_MAX_TOTAL_RAM)
        {
            printf("    !!!! Allocating more memory to %s would put it above it's maximum.\n", vcpu->domainName);
            return -1;
        }

        // Case 1.2 -  We need more RAM for this VM, but allocating more RAM
        //             would reduce the Host below its mimimum
        if ((hostFreeMemory - DOMAIN_MEMORY_INCREMENT) < MINIMUM_HOST_MEMORY)
        {
            printf("Setting needs more ram to 1\n");
            vcpu->needsMoreRAM = 1;
            printf("    !!!! Cannot allocate more memory to %s without reducing host below it's minimum!\n", vcpu->domainName);
            return -1;
        }

        // Case 1.3 -  The math works out, we're able to incrase the RAM
        //             available to this VM.
        printf("    - Increasing %s from %lluMB", vcpu->domainName, memory);
        unsigned long long increase = MIN(DOMAIN_MEMORY_INCREMENT, (DOMAIN_MAX_TOTAL_RAM-memory));
        //memory += MIN(DOMAIN_MEMORY_INCREMENT, (DOMAIN_TARGET_TOTAL_UPPER_BOUND-memory));
        memory += increase;

        printf(" to %lluMB of storage\n", memory);
        totalAllVMs += increase;

        int rva = virDomainSetMemory(vcpu->domain, memory*1024);

        if (rva == 0)
        {
            printf("    - set vcpu %s to have %lluMB of memory.\n", vcpu->domainName, memory);
            return -1;
        }
        else
        {
            printf("    - COULD NOT set vcpu %s to have %lluMB of memory.\n", vcpu->domainName, memory);
            return -1;
        }

        vcpu->needsMoreRAM = 0;
    }
    // Case 2:  This VM has way more RAM than it needs
    else if (unused > DOMAIN_MAX_FREE_RAM
            // OR if this VM can afford to give up a bit
            || (vmsNeedFreeRAM > 1 && unused > DOMAIN_MIN_FREE_RAM) )
    {
        // we need to decrement such that we do not let unused dip below
        // DOMAIN_MIN_FREE_RAM and total does not dip below DOMAIN_MIN_TOTAL_RAM

        unsigned long long decrement = MIN(DOMAIN_MEMORY_INCREMENT, MIN((memory - DOMAIN_MIN_TOTAL_RAM), unused-DOMAIN_MIN_FREE_RAM));

        memory -= decrement;
        printf("  -- Decreasing %s to %lluMB of storage\n", vcpu->domainName, memory);
        totalAllVMs -= decrement;

        int rva = virDomainSetMemory(vcpu->domain, memory*1024);

        if (rva == 0)
        {
            printf("    - set vcpu %s to have %lluMB of memory.\n", vcpu->domainName, memory);
            return -1;
        }
        else
        {
            printf("    - COULD NOT set vcpu %s to have %lluMB of memory.\n", vcpu->domainName, memory);
            return -1;
        }
    }
    // Case 3 -  Everything's fine :)
    else
    {
        printf ("  -- making no changes to %s!\n", vcpu->domainName);
    }

    return NO_ERROR;
}


int SetupHostMemoryStats(virConnectPtr conn)
{
    numberOfParams = 0;

    int rva = virNodeGetMemoryStats(conn, VIR_NODE_MEMORY_STATS_ALL_CELLS, NULL, &numberOfParams, 0);
    if (rva < 0)
    {
        printf("  - virGetHostMemoryStats failed.\n");
        return -1;
    }

    if (numberOfParams <= 0)
    {
        printf("  - virGetHostMemoryStats didn't get any params!\n");
        return -1;
    }
    else
    {
        printf("  - Got %d params\n", numberOfParams);
    }

    params = malloc(sizeof(virNodeMemoryStats) * numberOfParams);
    memset(params, '\0', sizeof(virNodeMemoryStats) * numberOfParams);

    RefreshHostMemoryStats(conn);

    return NO_ERROR;
}

int SetupVCPUsArray(virConnectPtr conn)
{
    printf("Setting up vCPU info:\n");

    virDomainPtr *domains;

    numDomains = virConnectListAllDomains(conn, &domains, VIR_CONNECT_LIST_DOMAINS_RUNNING | VIR_CONNECT_LIST_DOMAINS_PERSISTENT);

    if (numDomains <= 0)
    {
        printf("  - virConnectListAllDomains failed, error code: %d", numDomains);
        return -1;
    }

    printf("  - Found %d domains\n", numDomains);

    vcpus = malloc(sizeof(vCPUInfo) * numDomains);

    if (NULL == vcpus)
    {
        printf("  - Error:  malloc for space for vcpu failed.\n");
        return -1;
    }


    totalAllVMs = 0;

    for (int i = 0; i < numDomains; i++)
    {
        virDomainPtr domain = domains[i];
        vcpus[i].domainNumber = i;
        vcpus[i].domain = domain;
        vcpus[i].domainName = virDomainGetName(domain);
        vcpus[i].domainInfo = malloc(sizeof(virVcpuInfoPtr));
        vcpus[i].needsMoreRAM = 0;

        printf("  - Found domain %s:\n", vcpus[i].domainName);

        virDomainSetMemoryStatsPeriod(domain, 1, VIR_DOMAIN_AFFECT_CURRENT);

				vcpus[i].stats = malloc(sizeof(virDomainMemoryStatTags));
				RefreshDomainMemoryStats(&vcpus[i]);
    }

    return NO_ERROR;
}


/*
DO NOT CHANGE THE FOLLOWING FUNCTION
*/
void signal_callback_handler()
{
	printf("Caught Signal");
	is_exit = 1;
}

/*
DO NOT CHANGE THE FOLLOWING FUNCTION
*/
int main(int argc, char *argv[])
{
	virConnectPtr conn;

	if(argc != 2)
	{
		printf("Incorrect number of arguments\n");
		return 0;
	}

	// Gets the interval passes as a command line argument and sets it as the STATS_PERIOD for collection of balloon memory statistics of the domains
	int interval = atoi(argv[1]);

	conn = virConnectOpen("qemu:///system");
	if(conn == NULL)
	{
		fprintf(stderr, "Failed to open connection\n");
		return 1;
	}

	signal(SIGINT, signal_callback_handler);

	while(!is_exit)
	{
  		// Calls the MemoryScheduler function after every 'interval' seconds
  		MemoryScheduler(conn, interval);
  		sleep(interval);
	}

	// Close the connection
	virConnectClose(conn);
	return 0;
}

int Setup(virConnectPtr conn)
{
	  int rva =  SetupVCPUsArray(conn);
    if (NO_ERROR != rva)
    {
        return rva;
    }

    rva = SetupHostMemoryStats(conn);
    if (NO_ERROR != rva)
    {
        return rva;
    }

		return 0;
}

/*
COMPLETE THE IMPLEMENTATION
*/
void MemoryScheduler(virConnectPtr conn, int interval)
{

		static int firstTime = 1;
		if (1 == firstTime)
		{
				printf("-- PERFORMING FIRST TIME SETUP --\n\n");
				firstTime = 0;
				int setupRva = Setup(conn);
				if (NO_ERROR != setupRva)
				{
						printf("  !! Could not complete setup!\n");
						is_exit = 1;
						return;
				}
				printf("-- FIRST TIME SETUP COMPLETE --\n\n");
		}

    printf("MemoryScheduler: \n");

    unsigned long long totalUnused = 0;
    totalAllVMs = 0;

		for(int i = 0; i < numDomains; i++)
		{
				RefreshDomainMemoryStats(&vcpus[i]);
        totalUnused += vcpus[i].unused;
		}
    printf("Total across all VMs: %llu\n", totalAllVMs);


    for(int i = 0; i < numDomains; i++)
    {
        UpdateDomainMemoryAllocation(conn, &vcpus[i]);
    }

    printf("\n\n");
}

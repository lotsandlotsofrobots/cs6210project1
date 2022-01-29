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
#define ONE_CPU_PER_VPU 1

int is_exit = 0; // DO NOT MODIFY THIS VARIABLE

typedef struct pCPUInfo {
    int                cpuNumber;
    unsigned long long lastCPUTime;
    long long          relativeLoad;

    struct pCPUInfo * lowerLoad;
    struct pCPUInfo * higherLoad;
} pCPUInfo;

typedef struct vCPUInfo {
    int                domainNumber;
    virDomainPtr       domain;
    virVcpuInfoPtr     domainInfo;
    const char *       domainName;
    unsigned char *    cpuMap;
    unsigned long long startCPUTime;
    unsigned long long lastCPUTime;
    unsigned long long lastElapsed;
    long long          relativeLoad;

    struct vCPUInfo * lowerLoad;
    struct vCPUInfo * higherLoad;
} vCPUInfo;

pCPUInfo *pcpu = NULL;
vCPUInfo *vcpu = NULL;
int numDomains = 0;
int numPCPUs = 0;
int cpuMapSize = 0;
int cpusPerBlock = 0;

void CPUScheduler(virConnectPtr conn,int interval);

int GetNumPCPUs(virConnectPtr conn)
{
    char * c = virConnectGetCapabilities(conn);

    const char * startCPUsNum = strstr(c, "<cpus num='");

    if (NULL == startCPUsNum)
    {
        printf("Error:  Could not find start of cpus num substring.\n");
        if (NULL != c)
        {
            free(c);
        }

        return -1;
    }

    // advance the pointer to the first numerical character
    startCPUsNum = strstr(startCPUsNum, "'");
    startCPUsNum += 1;

    char * endCPUsNum = strstr(startCPUsNum, "'");

    if (NULL == endCPUsNum)
    {
        printf("Error:  Could not find end of cpus num substring.\n");
        if (NULL != c)
        {
            free(c);
        }
        return -1;
    }

    // hardcode an offset from startCPUs to get to the < character
    int numpCPUsSubstr = strtol(startCPUsNum, &endCPUsNum, 10);

    if (NULL != c)
    {
        free(c);
    }

    return numpCPUsSubstr;
}


int SetupPCPUsArray(virConnectPtr conn)
{
    printf("Setting up pCPU info:\n");

    numPCPUs = GetNumPCPUs(conn)/2;

    //cpuNumbersInLoadOrder = malloc(int * numPCPUs);

    if (-1 == numPCPUs)
    {
        return -1;
    }

    pcpu = malloc(sizeof(pCPUInfo) * numPCPUs);

    if (NULL == pcpu)
    {
        printf("  -  Error:  Malloc of space for pcpu failed.\n");
        return -1;
    }

    for (int i = 0; i < numPCPUs; i++)
    {
        pcpu[i].cpuNumber = i;
        pcpu[i].lastCPUTime = 0;
    }

    cpuMapSize = ceil(numPCPUs/8.);
    cpusPerBlock = MIN(numPCPUs,8);

    printf("  - NumPCPUs: %d\n", numPCPUs);
    printf("  - CPU map size is %d\n", cpuMapSize);

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

    vcpu = malloc(sizeof(vCPUInfo) * numDomains);

    if (NULL == vcpu)
    {
        printf("  - Error:  malloc for space for vcpu failed.\n");
        return -1;
    }

    for (int i = 0; i < numDomains; i++)
    {
        virDomainPtr domain = domains[i];
        vcpu[i].domainNumber = i;
        vcpu[i].domain = domain;
        vcpu[i].domainName = virDomainGetName(domain);
        vcpu[i].domainInfo = malloc(sizeof(virVcpuInfoPtr));

        printf("  - Found domain %s:\n", vcpu[i].domainName);

        int numInfo = virDomainGetVcpus(domain, vcpu[i].domainInfo, ONE_CPU_PER_VPU, NULL, 0);
        if (numInfo < 1)
        {
            printf("  - Error:  could not GetVCPU info for vCPU %d\n", i);
            return -1;
        }

        // for cpu map, we need one bit per cpu, so ceil(vcpu/8) bytes

        vcpu[i].cpuMap = malloc(cpuMapSize * sizeof(unsigned char));

        virDomainGetVcpus(domain, vcpu[i].domainInfo, 1, vcpu[i].cpuMap, cpuMapSize);

        printf("    - Current cpuMap:  ");
        printf("%u", vcpu[i].cpuMap[0]);
        printf("\n");
    }

    return NO_ERROR;
}

// map right off the bat, so we can fairly accurately know how long things
// have run on each CPU - if they were assigned no affinity at all, the first
// time we take our samples, they may have been all over the place

int PerformInitialVCPUtoPCPUMapping(virConnectPtr conn)
{
    printf("Performing initial VCPU affinity mapping:\n");

    // iterate over them, doing a serpentine draft
    // (if doubling up is necessary, person with
    // heaviest also gets lightest)
    int reverse = 0;

    for (int i = 0; i < numDomains; i++)
    {
        virDomainPtr domain = vcpu[i].domain;
        //virDomainSuspend(domain);

        // we need to generate a 00100000 style bit field
        // but need to cycle over ALL cpus
        //   - might be more than 8, might need to cycle to beginning

      //  int cpuMapBlock = (i / numPCPUs) % cpuMapSize;
      /*  int cpuMapBlock = i/
        int cpuInsideBlock = (i % numPCPUs);

        int absoluteCPU = cpuMapBlock*cpuMapSize + cpuInsideBlock;
        unsigned int power = (unsigned int) absoluteCPU/numPCPUs;
        int reverse = pow(-1, (power%2));

        if (1 == reverse)
        {
            cpuInsideBlock = abs(cpuInsideBlock - );
        }

        printf("%d / %u = Power: %d\n", absoluteCPU, numPCPUs, power);
        printf("Reverse: %d\n", reverse);
*/
        int cpuNumber = i % numPCPUs;
        int reverse = (i/numPCPUs) % 2;
        cpuNumber = abs(numPCPUs-cpuNumber-1 - (numPCPUs-1)*reverse);

        int blockBumber = cpuNumber / cpusPerBlock;
        int bitInBlock = cpuNumber % cpusPerBlock;

        for(int j = 0; j < cpuMapSize; j++)
        {
            vcpu[i].cpuMap[j] = 0;
        }

        vcpu[i].cpuMap[blockBumber] = (1 << bitInBlock);

        virDomainPinVcpu(domain, 0, vcpu[i].cpuMap, cpuMapSize);

        printf("  - Assigned domain %d to pcpu %d\n", i, cpuNumber);
    }

    // resume all at the same time for more accurate numbers
    for (int i = 0; i < numDomains; i++)
    {
        //virDomainPtr domain = vcpu[i].domain;
        //virDomainResume(domain);

        vcpu[i].startCPUTime = vcpu[i].domainInfo->cpuTime;
        vcpu[i].lastCPUTime = vcpu[i].startCPUTime;
        printf("    - Domain %d cpu time: %llu on %d\n", i, vcpu[i].domainInfo->cpuTime, vcpu[i].domainInfo->cpu);
    }

    return NO_ERROR;
}


int Setup(virConnectPtr conn)
{
    int rva = 0;
    rva = SetupPCPUsArray(conn);
    if (NO_ERROR != rva)
    {
        return rva;
    }

    rva = SetupVCPUsArray(conn);
    if (NO_ERROR != rva)
    {
        return rva;
    }

    rva = PerformInitialVCPUtoPCPUMapping(conn);

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



	// Get the total number of pCpus in the host
	signal(SIGINT, signal_callback_handler);


	while(!is_exit)
	// Run the CpuScheduler function that checks the CPU Usage and sets the pin at an interval of "interval" seconds
	{
		CPUScheduler(conn, interval);
		sleep(interval);
	}

	// Closing the connection
	virConnectClose(conn);
	return 0;
}

/*
void CreatePCPULoadTree(pCPUInfo * head)
{
    pCPUInfo * node = NULL;

    for (int i = 1; i < numPCPUs; i++)
    {
        node = head;

        while(1)
        {
            if ( pcpu[i].relativeLoad < node->relativeLoad )
            {
                if (node->lowerLoad == NULL)
                {
                    node->lowerLoad = &pcpu[i];
                    break;
                }
                else
                {
                    node = node->lowerLoad;
                }
            }
            else if ( pcpu[i].relativeLoad >= node->relativeLoad )
            {
                if (node->higherLoad == NULL)
                {
                    node->higherLoad = &pcpu[i];
                    break;
                }
                else
                {
                    node = node->higherLoad;
                }
            }
        }
    }
}

void TreeToArray(pCPUInfo * node, int * array, int index)
{
    if (node->lowerWeight != NULL)

}
*/


void InsertPCPUIntoLoadList(pCPUInfo ** head, pCPUInfo * newNode)
{
  /*
    long long difference = newNode->lastCPUTime - averageCPUTime;
    printf("  -- Difference between CPU %d and average: %lld\n", i, difference);
    newNode->relativeLoad = difference;
*/
    if (*head == NULL)
    {
        *head = newNode;
        printf("Inserting %d at head\n", newNode->cpuNumber);
    }
    else
    {
        pCPUInfo * node = *head;
        while (newNode->relativeLoad > node->relativeLoad)
        {
            // we are higher load than this node,
            // and it has a neighbor node
            if (node->higherLoad != NULL)
            {
                node = node->higherLoad;
            }
            else
            {
                // if we are inside this, we're higher load,
                // but there is no node to move to
                break;
            }
        }

        // we broke, 3 options:
        //  - we are the smallest (replace head)
        //  - we are the largest (append to the end)
        //  - we are in between two nodes (insert in between them)
        if (node == *head)
        {
            printf("Replacing head, used to be %d(%lld), now %d(%lld)\n", node->cpuNumber, node->relativeLoad, newNode->cpuNumber, newNode->relativeLoad );
            node->lowerLoad = newNode;
            newNode->higherLoad = node;
            *head = newNode;
        }
        // we are higher load than this node,
        // but it has no neighbor (we're becoming tail)
        else if (NULL == node->higherLoad)
        {
            printf("Node->higherLoad is null, inserting %d(%lld) here AFTER %d(%lld)\n", newNode->cpuNumber, newNode->relativeLoad, node->cpuNumber, node->relativeLoad);
            node->higherLoad = newNode;
            newNode->lowerLoad = node;
        }
        else
        {
            pCPUInfo * prev = node->lowerLoad;

            node->lowerLoad = newNode;
            prev->higherLoad = newNode;

            newNode->lowerLoad = prev;
            newNode->higherLoad = node;
        }
    }
}


void InsertVCPUIntoLoadList(vCPUInfo ** head, vCPUInfo * newNode)
{
    if (*head == NULL)
    {
        *head = newNode;
        printf("Inserting %d at head\n", newNode->domainNumber);
    }
    else
    {
        vCPUInfo * node = *head;
        while (newNode->relativeLoad > node->relativeLoad)
        {
            // we are higher load than this node,
            // and it has a neighbor node
            if (node->higherLoad != NULL)
            {
                node = node->higherLoad;
            }
            else
            {
                // if we are inside this, we're higher load,
                // but there is no node to move to
                break;
            }
        }

        // we broke, 3 options:
        //  - we are the smallest (replace head)
        //  - we are the largest (append to the end)
        //  - we are in between two nodes (insert in between them)
        if (node == NULL)
        {
            printf("Something went horribly wrong.\n");
        }

        if (node == *head)
        {
            if(newNode == NULL)
            {
                printf("Something else went horribly wrong.\n");
            }
            if(node == NULL)
            {
                printf("???\n");
            }

            printf("Replacing head, used to be %d(%lld), now %d(%lld)\n", node->domainNumber, node->relativeLoad, newNode->domainNumber, newNode->relativeLoad );
            node->lowerLoad = newNode;
            newNode->higherLoad = node;
            *head = newNode;
        }
        // we are higher load than this node,
        // but it has no neighbor (we're becoming tail)
        else if (NULL == node->higherLoad)
        {
          if(newNode == NULL)
          {
              printf("Something very else went horribly wrong.\n");
          }
          else if (node == NULL)
          {
            printf("ummm.\n");
          }

            printf("Node->higherLoad is null, inserting %d(%lld) here AFTER %d(%lld)\n", newNode->domainNumber, newNode->relativeLoad, node->domainNumber, node->relativeLoad);
            node->higherLoad = newNode;
            newNode->lowerLoad = node;
        }
        else
        {
          if(newNode == NULL)
          {
              printf("wtf.\n");
          }
          else if (node == NULL)
          {
              printf("how did this happen.\n");
          }

            vCPUInfo * prev = node->lowerLoad;

            prev->higherLoad = newNode;

            newNode->lowerLoad = prev;
            newNode->higherLoad = node;

            node->lowerLoad = newNode;
        }
    }
}



/* COMPLETE THE IMPLEMENTATION */
void CPUScheduler(virConnectPtr conn, int interval)
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

    printf("Checking cpu loads:\n");

    unsigned long long averageElapsed = 0;
    unsigned long long averageCPUTime = 0;

    //long long cpuRelativeLoad[numPCPUs];
    //long long vcpuRelativeWeight[numDomains];

    for (int i = 0; i < numPCPUs; i++)
    {
        pcpu[i].lastCPUTime = 0;
    }

    for (int i = 0; i < numDomains; i++)
    {
        virDomainPtr domain = vcpu[i].domain;
        int numInfo = virDomainGetVcpus(domain, vcpu[i].domainInfo, 1, NULL, 0);
        if (numInfo < 1)
        {
            printf("  !! Failed to get info for domain %d, skipping?\n", i);
            continue;
        }

        int activeCPU = vcpu[i].domainInfo->cpu;
        //unsigned long long totalElapsedForVCPU = (vcpu[i].domainInfo->cpuTime - vcpu[i].startCPUTime);

        unsigned long long elapsed = (vcpu[i].domainInfo->cpuTime - vcpu[i].lastCPUTime);
        vcpu[i].lastElapsed = elapsed;
        vcpu[i].lastCPUTime = vcpu[i].domainInfo->cpuTime;
        vcpu[i].lowerLoad = NULL;
        vcpu[i].higherLoad = NULL;

        printf("  -- Domain %d elapsed cpu time: %llu (on %d)\n", i, elapsed, activeCPU);
        pcpu[activeCPU].lastCPUTime += elapsed;

        // shift everyone by one
        //int cpuMapBlock = ((activeCPU+1) / numPCPUs) % cpuMapSize;
        //int cpuInsideBlock = ((activeCPU+1) % numPCPUs);

        //printf("    -> Moving %d from %d to %d\n", i, activeCPU, cpuMapBlock*cpuMapSize + cpuInsideBlock);
        //vcpu[i].cpuMap[cpuMapBlock] = (1 << cpuInsideBlock);

        // divide later, save time now
        averageElapsed += elapsed;
    }

    for (int i = 0; i < numPCPUs; i++)
    {
        printf("  -- CPU %d lastCPUTime: %llu\n", i, pcpu[i].lastCPUTime);
        averageCPUTime += pcpu[i].lastCPUTime;
        pcpu[i].lowerLoad = NULL;
        pcpu[i].higherLoad = NULL;
    }

    averageElapsed /= numDomains;
    averageCPUTime /= numPCPUs;

    /*  int highestToLowestLoadCPUs[numPCPUs];
    for (int i = 0; i < numPCPUs; i++)
    {
        highestToLowestLoadCPUs[i] = 0;
    } */

    // find our load relative to others
    int needToRebalance = 0;
    long long rebalanceThreshold = averageCPUTime * 0.05;

    pCPUInfo * pCPUHead = NULL;

    for (int i = 0; i < numPCPUs; i++)
    {
        long long difference = pcpu[i].lastCPUTime - averageCPUTime;
        printf("  -- Difference between CPU %d and average: %lld\n", i, difference);
        pcpu[i].relativeLoad = difference;

        InsertPCPUIntoLoadList(&pCPUHead, &pcpu[i]);

        if (abs(difference) > rebalanceThreshold)
        {
            printf("  -- Rebalance triggered.\n");
            needToRebalance = 1;
        }
    }

    // print the tree:
    printf("    -> Printing the load tree\n");
    pCPUInfo * cpunode = pCPUHead;

    //for (int i = 0; i < numPCPUs; i++)
    int cpuNumbersInLoadOrder[numPCPUs];
    for(int i = 0; i < numPCPUs; i++)
    {
        printf("      -> CPU %d, relativeLoad: %lld\n", cpunode->cpuNumber, cpunode->relativeLoad);
        cpuNumbersInLoadOrder[i] = cpunode->cpuNumber;
        cpunode = cpunode->higherLoad;
    }

    if (needToRebalance == 0)
    {
        printf("  -- No need to rebalance.\n");
        return;
    }


    // do basically the same thing as we did before
    vCPUInfo * vCPUHead = NULL;
    for (int i = 0; i < numDomains; i++)
    {
        long long difference = vcpu[i].lastElapsed - averageElapsed;
        printf("  -- Difference between vCPU %d and average: %lld\n", i, difference);
        vcpu[i].relativeLoad = difference;

        InsertVCPUIntoLoadList(&vCPUHead, &vcpu[i]);
    }

    // iterate through, assigning to CPUs
    vCPUInfo * vcpunode = vCPUHead;
    int vcpuNumbersInLoadOrder[numDomains];
    for (int i = 0; i < numDomains; i++)
    {
        printf("      -> vCPU %d (%s), relativeLoad: %lld\n", vcpunode->domainNumber, vcpunode->domainName, vcpunode->relativeLoad);
        vcpuNumbersInLoadOrder[i] = vcpunode->domainNumber;
        vcpunode = vcpunode->higherLoad;
    }

    // finally, assign them to pCPUs
    for (int i = numDomains-1; i >= 0; i--)
    {
        int vpuID = vcpuNumbersInLoadOrder[i];
        int cpuNumber = i % numPCPUs;
        int reverse = (i/numPCPUs) % 2;
        cpuNumber = abs(numPCPUs-cpuNumber-1 - (numPCPUs-1)*reverse);

        int blockBumber = cpuNumber / cpusPerBlock;
        int bitInBlock = cpuNumber % cpusPerBlock;

        for(int j = 0; j < cpuMapSize; j++)
        {
            vcpu[i].cpuMap[j] = 0;
        }

        vcpu[i].cpuMap[blockBumber] = (1 << bitInBlock);
        //printf("+++ Assigning vcpu %d (%s) to pCPU %d\n", vcpu[vpuID].domainNumber, vcpu[vpuID].domainName, blockBumber*cpuMapSize + bitInBlock);
        printf("+++ Assigning vcpu %d (%s) to pCPU %d\n", vcpu[vpuID].domainNumber, vcpu[vpuID].domainName, cpuNumber);

      /*
        int vpuID = vcpuNumbersInLoadOrder[i];
        int cpuMapBlock = (i / numPCPUs) % cpuMapSize;
        int cpuInsideBlock = (i % numPCPUs);

        //vcpu[i].domain = pow(2,i)(i % numPCPUs));
        for(int j = 0; j < cpuMapSize; j++)
        {
            vcpu[vpuID].cpuMap[j] = 0;
        }

        printf("+++ Assigning vcpu %d (%s) to pCPU %d\n", vcpu[vpuID].domainNumber, vcpu[vpuID].domainName, cpuMapBlock*cpuMapSize + cpuInsideBlock);

        vcpu[vpuID].cpuMap[cpuMapBlock] = (1 << cpuInsideBlock);
*/
        virDomainPinVcpu(vcpu[vpuID].domain, 0, vcpu[vpuID].cpuMap, cpuMapSize);
    }


}

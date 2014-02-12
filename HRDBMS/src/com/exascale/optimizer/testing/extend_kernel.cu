#include <string.h>
#include <stdlib.h>

__device__ int myStrlen(char* string)
{
	int cnt = 0;
	char* temp = string;
	while (*temp != 0)
	{
		cnt++;
		temp++;
	}
	
	return cnt;
}

__device__ int parseLong(char* string)
{
	char* temp = string;
	int negative = 0;
	int offset = 0;
	long result = 0;
	int length = myStrlen(string);
		
	if (*temp == '-')
	{
		negative = 1;
		offset = 1;
	}
		
	while (offset < length)
	{
		char b = temp[offset];
		b -= 48;
		result *= 10;
		result += b;
		offset++;
	}
		
	if (negative != 0)
	{
		result *= -1;
	}
		
	return result;
}

__device__ double myStrtod(char* string)
{
	char newTemp[32];
	char* temp = string;
	int p = -1;
	while (*temp != 0 && p == -1)
	{
		if (*temp == '.')
		{
			p = temp - string;
		}
		
		temp++;
	}
	
	temp = string;
	if (p < 0)
	{
		return parseLong(string);
	}
		
	int negative = 0;
	int offset = 0;
	if (*temp == '-')
	{
		negative = 1;
		offset = 1;
	}
	
	int strlen = myStrlen(temp);
	int i = 0;
	while (i < p)
	{
		newTemp[i] = temp[i];
		i++;
	}
	
	i++;
	while (i < strlen)
	{
		newTemp[i-1] = temp[i];
		i++;
	}
	
	temp = newTemp;
	long n = parseLong(temp);
	int x = strlen - p - offset;
	i = 0;
	long d = 1;
	while (i < x)
	{
		d *= 10;
		i++;
	}
		
	double retval = (n*1.0) / (d*1.0);
	if (negative != 0)
	{
		retval *= -1;
	}
	
	return retval;
}

__global__ void doExtendKernel(double* deviceRows, char* parseStack, double* deviceResults, int numJobs, int numCols, int numPrefixes, int prefixBytesLength)
{
	char execStack[4096];
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < numJobs)
    {
		int parseStackPtr = 0;
		int parseStackProcessed = 0;
		int execStackPtr = 0;
		int rowsCntr = 0;

		while (parseStackProcessed < numPrefixes)
		{ 
			char* temp = parseStack + parseStackPtr;
			if (*temp == '*') 
			{
				execStackPtr -= sizeof(double);
				double lhs = *(double*)(execStack + execStackPtr);
				execStackPtr -= sizeof(double);
				double rhs = *(double*)(execStack + execStackPtr);
				*(double*)(execStack + execStackPtr) = lhs * rhs;
				execStackPtr += sizeof(double);
				parseStackPtr += 2;
				parseStackProcessed += 1;
			} 
			else if (*temp == '-') 
			{
				execStackPtr -= sizeof(double);
				double lhs = *(double*)(execStack + execStackPtr);
				execStackPtr -= sizeof(double);
				double rhs = *(double*)(execStack + execStackPtr);
				*(double*)(execStack + execStackPtr) = lhs - rhs;
				execStackPtr += sizeof(double);
				parseStackPtr += 2;
				parseStackProcessed += 1;
			} 
			else if (*temp == '+') 
			{
				execStackPtr -= sizeof(double);
				double lhs = *(double*)(execStack + execStackPtr);
				execStackPtr -= sizeof(double);
				double rhs = *(double*)(execStack + execStackPtr);
				*(double*)(execStack + execStackPtr) = lhs + rhs;
				execStackPtr += sizeof(double);
				parseStackPtr += 2;
				parseStackProcessed += 1;
			} 
			else if (*temp == '/') 
			{
				execStackPtr -= sizeof(double);
				double lhs = *(double*)(execStack + execStackPtr);
				execStackPtr -= sizeof(double);
				double rhs = *(double*)(execStack + execStackPtr);
				*(double*)(execStack + execStackPtr) = lhs / rhs;
				execStackPtr += sizeof(double);
				parseStackPtr += 2;
				parseStackProcessed += 1;
			} 
			else 
			{
				if ((*temp >= 'a' && *temp <= 'z') || (*temp >= 'A' && *temp <= 'Z') || (*temp == '_')) 
				{
					*(double*)(execStack + execStackPtr) = deviceRows[rowsCntr + idx * numCols];
					rowsCntr++;
					execStackPtr += sizeof(double);
					parseStackPtr += (1 + myStrlen(temp));
					parseStackProcessed++;
				} 
				else 
				{
					double d = myStrtod(temp);
					*(double*)(execStack + execStackPtr) = d;
					execStackPtr += sizeof(double);
					parseStackPtr += (1 + myStrlen(temp));
					parseStackProcessed++;
				}
			}
		}

		execStackPtr -= sizeof(double);
		deviceResults[idx] = *(double*)(execStack + execStackPtr);
	}
}

void cudaExtend(double* nativeRows, char* nativePrefix, double* nativeResults, int numJobs, int numCols, int numPrefixes, int prefixBytesLength)
{
	double* deviceResults;
	char* devicePrefix;
	double* deviceRows;
	//cuda malloc deviceResults
	cudaMalloc((void**)&deviceResults, numJobs * sizeof(double)); 
	//cuda malloc prefix
	cudaMalloc((void**)&devicePrefix, prefixBytesLength);
	//cuda memcpy prefix
	cudaMemcpy(devicePrefix, nativePrefix, prefixBytesLength, cudaMemcpyHostToDevice);
	//cuda malloc rows
	cudaMalloc((void**)&deviceRows, sizeof(double) * numJobs * numCols);
	//cuda memcpy rows
	cudaMemcpy(deviceRows, nativeRows, sizeof(double) * numJobs * numCols, cudaMemcpyHostToDevice);
	//invoke kernel
	int blockSize = 128;
	int nBlocks = numJobs/blockSize + (numJobs%blockSize == 0?0:1);
	doExtendKernel <<< nBlocks, blockSize >>> (deviceRows, devicePrefix, deviceResults, numJobs, numCols, numPrefixes, prefixBytesLength);
	//copy deviceResults back to nativeResults
	cudaMemcpy(nativeResults, deviceResults, numJobs * sizeof(double), cudaMemcpyDeviceToHost);
	cudaFree(deviceRows);
	cudaFree(devicePrefix);
	cudaFree(deviceResults);
}

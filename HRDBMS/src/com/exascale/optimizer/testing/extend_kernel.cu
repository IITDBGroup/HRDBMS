#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#define gpuErrchk(ans) { gpuAssert((ans), __FILE__, __LINE__); }

extern "C"
{
__constant__ char parseStack[4096];

inline void gpuAssert(cudaError_t code, char *file, int line, bool abort=true)
{
   if (code != cudaSuccess) 
   {
      fprintf(stderr,"GPUassert: %s %s %d\n", cudaGetErrorString(code), file, line);
      if (abort) exit(code);
   }
}

__device__ int myStrlen(char* string)
{
	char* temp = string;
	while (*temp != 0)
	{
		temp++;
	}
	
	return temp-string;
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

__device__ float myStrtod(char* string)
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
		
	float retval = (n*1.0f) / (d*1.0f);
	if (negative != 0)
	{
		retval *= -1.0f;
	}
	
	return retval;
}

__global__ void doExtendKernel(float* deviceRows, float* deviceResults, int numJobs, int numCols, int numPrefixes, int prefixBytesLength, float* execStack)
{
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < numJobs)
    {
    	int parseStackPtr = 0;
		int parseStackProcessed = 0;
		int esp = 512 * idx;
		int rowsCntr = 0;

		while (parseStackProcessed < numPrefixes)
		{ 
			char* temp = parseStack + parseStackPtr;
			if (*temp == '*') 
			{
				esp--;
				float lhs = execStack[esp];
				esp--;
				float rhs = execStack[esp];
				execStack[esp] = lhs * rhs;
				esp++;
				parseStackPtr += 2;
				parseStackProcessed += 1;
			} 
			else if (*temp == '-') 
			{
				esp--;
				float lhs = execStack[esp];
				esp--;
				float rhs = execStack[esp];
				execStack[esp] = lhs - rhs;
				esp++;
				parseStackPtr += 2;
				parseStackProcessed += 1;
			} 
			else if (*temp == '+') 
			{
				esp--;
				float lhs = execStack[esp];
				esp--;
				float rhs = execStack[esp];
				execStack[esp] = lhs + rhs;
				esp++;
				parseStackPtr += 2;
				parseStackProcessed += 1;
			} 
			else if (*temp == '/') 
			{
				esp--;
				float lhs = execStack[esp];
				esp--;
				float rhs = execStack[esp];
				execStack[esp] = lhs / rhs;
				esp++;
				parseStackPtr += 2;
				parseStackProcessed += 1;
			} 
			else 
			{
				if ((*temp >= 'a' && *temp <= 'z') || (*temp >= 'A' && *temp <= 'Z') || (*temp == '_')) 
				{
					execStack[esp] = deviceRows[rowsCntr + idx * numCols];
					rowsCntr++;
					esp++;
					parseStackPtr += (1 + myStrlen(temp));
					parseStackProcessed++;
				} 
				else 
				{
					float d = myStrtod(temp);
					execStack[esp] = d;
					esp++;
					parseStackPtr += (1 + myStrlen(temp));
					parseStackProcessed++;
				}
			}
		}

		esp--;
		deviceResults[idx] = execStack[esp];
	}
}

void cudaExtend(float* nativeRows, char* nativePrefix, float* nativeResults, int numJobs, int numCols, int numPrefixes, int prefixBytesLength)
{
	float* deviceResults;
	float* deviceRows;
	//cuda malloc deviceResults
	gpuErrchk(cudaMalloc((void**)&deviceResults, numJobs * sizeof(float))); 
	//gpuErrchk(cudaMemset((void*)deviceResults, 0xFE, numJobs * sizeof(float)));
	//cuda memcpy prefix
	gpuErrchk(cudaMemcpyToSymbol(parseStack, nativePrefix, prefixBytesLength));
	//cuda malloc rows
	gpuErrchk(cudaMalloc((void**)&deviceRows, sizeof(float) * numJobs * numCols));
	//cuda memcpy rows
	gpuErrchk(cudaMemcpy(deviceRows, nativeRows, sizeof(float) * numJobs * numCols, cudaMemcpyHostToDevice));
	float* execStack;
	gpuErrchk(cudaMalloc((void**)&execStack, sizeof(float) * 512 * numJobs));
	//invoke kernel
	int blockSize = 128;
	int nBlocks = numJobs/blockSize + (numJobs%blockSize == 0?0:1);
	doExtendKernel <<< nBlocks, blockSize >>> (deviceRows, deviceResults, numJobs, numCols, numPrefixes, prefixBytesLength, execStack);
	//copy deviceResults back to nativeResults
	gpuErrchk(cudaPeekAtLastError());
	gpuErrchk(cudaMemcpy(nativeResults, deviceResults, numJobs * sizeof(float), cudaMemcpyDeviceToHost));
	gpuErrchk(cudaFree(deviceRows));
	gpuErrchk(cudaFree(deviceResults));
	gpuErrchk(cudaFree(execStack));
}
}
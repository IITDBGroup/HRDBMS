#include "com_exascale_optimizer_testing_Rootbeer.h"

void cudaExtend(float*, char*, float*, int, int, int, int);

JNIEXPORT void JNICALL Java_com_exascale_optimizer_testing_Rootbeer_extendKernel
  (JNIEnv * env, jobject thisPointer, jfloatArray rows, jbyteArray prefix, jfloatArray results, jint numJobs, jint numCols, jint numPrefixes, jint prefixBytesLength)
{
	//convert to float* rows
	float* nativeRows = (*env)->GetFloatArrayElements(env, rows, 0);
	//char* prefix
	char* nativePrefix = (*env)->GetByteArrayElements(env, prefix, 0);
	//float* results
	float* nativeResults = (*env)->GetFloatArrayElements(env, results, 0);
	
	cudaExtend(nativeRows, nativePrefix, nativeResults, numJobs, numCols, numPrefixes, prefixBytesLength);
	
	(*env)->ReleaseFloatArrayElements(env, rows, nativeRows, 0);
	(*env)->ReleaseByteArrayElements(env, prefix, nativePrefix, 0);
	(*env)->ReleaseFloatArrayElements(env, results, nativeResults, 0);
}

#include "com_exascale_optimizer_testing_Rootbeer.h"
void cudaExtend(double*, char*, double*, int, int, int, int);

JNIEXPORT void JNICALL Java_com_exascale_optimizer_testing_Rootbeer_extendKernel
  (JNIEnv * env, jobject thisPointer, jdoubleArray rows, jbyteArray prefix, jdoubleArray results, jint numJobs, jint numCols, jint numPrefixes, jint prefixBytesLength)
{
	//convert to double* rows
	double* nativeRows = (*env)->GetDoubleArrayElements(env, rows, 0);
	//char* prefix
	char* nativePrefix = (*env)->GetByteArrayElements(env, prefix, 0);
	//double* results
	double* nativeResults = (*env)->GetDoubleArrayElements(env, results, 0);
	cudaExtend(nativeRows, nativePrefix, nativeResults, numJobs, numCols, numPrefixes, prefixBytesLength);
	(*env)->ReleaseDoubleArrayElements(env, rows, nativeRows, 0);
	(*env)->ReleaseByteArrayElements(env, prefix, nativePrefix, 0);
	(*env)->ReleaseDoubleArrayElements(env, results, nativeResults, 0);
}

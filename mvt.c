#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <math.h>

#define MAX_FILE_NAME_LEN 10
#define MAX_N 10000 
#define MAX_K 10

int *midresult_arr;
int file_no;
int matrix_dimension;
void *reducer_fn(void *param);
void *mapper_fn(int param);
int vector_arr[MAX_N];
char file_name[MAX_FILE_NAME_LEN];
FILE *fp_mapperfile;
pthread_mutex_t mutex1 = PTHREAD_MUTEX_INITIALIZER;

void * reducer_fn (void *resultfile)
{
	//mapper threads
  pthread_t tid_m[file_no]; 
  int c2;
 	for (c2 = 0; c2 < file_no; ++c2)
 	{
    pthread_create(&tid_m[c2], NULL, (void *) mapper_fn, c2+1);
 	}
 	//wait for mapper threads
  for (c2 = 0; c2 < file_no; ++c2)
  {
  	pthread_join(tid_m[c2], NULL);
  }

  //generate file
  FILE *fp_resultfile = fopen(resultfile,"w");

  for( c2=0; c2 < matrix_dimension; ++c2)
  {
  	fprintf(fp_resultfile, "%d\t", c2+1);
  	fprintf(fp_resultfile, "%d\n", midresult_arr[c2]);
  }
  fclose(fp_resultfile);
  pthread_exit(0);
}

void * mapper_fn (int no_of_file)
{
 	//generate data
  int row_no;
  int no;
  pthread_mutex_lock( &mutex1 );
  int col_no = 0;
  sprintf(file_name, "file_%d", no_of_file);
  fp_mapperfile = fopen(file_name,"r");
  while ((fscanf (fp_mapperfile, "%d", &no)) != EOF)
  {
  	row_no = no;
  	if (fscanf (fp_mapperfile, "%d", &no) != EOF)
  	{
  		col_no = no;
  	}
  	if (fscanf (fp_mapperfile, "%d", &no) != EOF)
  	{
  		midresult_arr[row_no-1] += no * vector_arr[col_no-1];
  	}
  }
  pthread_mutex_unlock( &mutex1 );
 	fflush (stdout); 
	pthread_exit(0); 
}

int main(int argc, char *argv[]) 
{	  
  //variables used
  int k;
  int no;
  int i;
  int s;
  int l;
  FILE *fp_matrixfile, *fp_matrixfile1, *fp_vectorfile, *fp_vectorfile1, *k_files;
  struct timeval tvs;
  struct timeval tve;
  unsigned long reportedTime = 0;

  gettimeofday(&tvs, NULL); 
  //check of argument number
  if (argc != 5)
  {
  	printf("user must enter correct number of arguments\n");
  	return 0;
  }

  k = atoi(argv[4]);
  fp_matrixfile = fopen(argv[1],"r");
  fp_matrixfile1 = fopen(argv[1],"r");
  fp_vectorfile = fopen(argv[2],"r");
  fp_vectorfile1 = fopen(argv[2],"r");

  //check if file ptrs are null
  if (fp_matrixfile == NULL || fp_vectorfile == NULL)
  {
  	printf("input file is NULL\n");
  	return 0;
  }

 	i = 0;
  l = 0;
  //get L
  while ( (fscanf (fp_matrixfile, "%d", &no)) != EOF ) 
  {
   	i++;
   	if (i%3 == 0)
   	{
   		l++;
   	}
  }
  s = ceil( (float)l/k );

  //create vector array 
  matrix_dimension = 0;
  while ((fscanf (fp_vectorfile, "%d", &no)) != EOF)
  {
  	matrix_dimension++;
  }
  matrix_dimension = matrix_dimension/2;
  	
  i = 0;
 	while ( fscanf (fp_vectorfile1, "%d", &no) != EOF && fscanf (fp_vectorfile1, "%d", &no) != EOF ) //to dodge row val
  {
  	vector_arr[i] = no;
  	i++;
  }

  //create K files
  file_no = 1;
  i = 0;
  k_files = NULL;
  while ( (fscanf (fp_matrixfile1, "%d", &no)) != EOF ) 
  {
  	if ( i%(3*s) == 0 )
  	{
  		if (k_files != NULL)
  		{
  			fclose(k_files);
  			k_files = NULL;
  		}
  		//create file
  		sprintf(file_name, "file_%d", file_no);
  		k_files = fopen(file_name, "w");
  			
  		if ( k_files == NULL)
  		{
  			printf("file %d couldn't be created!\n", file_no);
  		}
  		file_no++;
  	}
  	i++;
  		
  	//fill table
  	fprintf(k_files, "%d\t", no);
  	if (i%3 == 0)
   	{
  		fputs("\n", k_files);
   	}
    	
  }
	fclose(k_files);
  file_no--;

  midresult_arr=(int*)calloc(matrix_dimension,sizeof(int));
  //reducer thread
  pthread_t tid_r;
  pthread_create(&tid_r, NULL, (void *) reducer_fn, argv[3]);
  pthread_join(tid_r, 0);
  printf("program done\n");
  //calculate time
  gettimeofday(&tve, NULL);
  reportedTime = tve.tv_usec - tvs.tv_usec;
  //printf("COST OF MVT.C \nmatrix dimension : %d\nk : %d\ntime elapsed: %ld\n", matrix_dimension, k, reportedTime); 

 	exit(0);
}
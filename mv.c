#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#define MAX_FILE_NAME_LEN 10

int main(int argc, char *argv[]) //matrixfile vectorfile resultfile k
{
  printf ("mv started\n"); 

  //variables used
  int k;
  int row_no = 0;
  int col_no = 0; 
  int val = 0;
  int no;
  int i;
  int s;
  int l;
  int file_no;
  char file_name[MAX_FILE_NAME_LEN];
  pid_t pid;
  FILE *fp_matrixfile, *fp_matrixfile1, *fp_mapperfile, *fp_vectorfile, *fp_vectorfile1, *fp_resultfile, *k_files,
  *fp_intermediate, *fp_intermediate1, *fp_intermediate2;
  int matrix_dimension;
  int c1;
  int intermediate_val;
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
  int vector_arr[matrix_dimension];

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
  	
  //assign k files to mappers
  for (i=0; i<file_no; ++i)
  {
  	pid = fork();
  	if (pid == 0)
  	{
  		//logic for each mapper
  		int *midresult_arr=(int*)calloc(matrix_dimension,sizeof(int));
  		sprintf(file_name, "file_%d", i+1);
  		fp_mapperfile = fopen(file_name,"r");
  		col_no = 0;
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

  		//create  intermediate results file
  		sprintf(file_name, "intermediate_file_%d", i+1);
  		fp_intermediate = fopen(file_name, "w");
  		if ( fp_intermediate == NULL)
  		{
  			printf("file %d couldn't be created!\n", i+1);
  		}

  		for (c1 = 0; c1 < matrix_dimension; c1++ ) 
  		{
  			if (midresult_arr[c1] != 0)
  			{
  				fprintf(fp_intermediate, "%d\t", c1+1);
  				fprintf(fp_intermediate, "%d\n", midresult_arr[c1]);
  			}
  		}
  		exit(0);
  	}
  }
  for (i=0; i<file_no; ++i)
  {
  	wait();
  }

  //reducer logic
  fp_resultfile = fopen(argv[3],"w");
  pid = fork();
  if(pid == 0)
  {
  	//merge files
  	int *result_arr=(int*)calloc(matrix_dimension,sizeof(int));
  	for (i = 0; i < file_no; ++i)
  	{
  		sprintf(file_name, "intermediate_file_%d", i+1);
  		fp_intermediate1 = fopen(file_name,"r");
  		while(fscanf (fp_intermediate1, "%d", &no) != EOF)
  		{	
  			row_no = no;
  			if (fscanf (fp_intermediate1, "%d", &no) != EOF)
  			{
  				result_arr[row_no-1] += no;
  			}
  		}
  	}
  	//write array to file
  	for( i=0; i < matrix_dimension; ++i)
  	{
  		fprintf(fp_resultfile, "%d\t", i+1);
  		fprintf(fp_resultfile, "%d\n", result_arr[i]);
  	}
  	exit(0);
  }

  fclose(fp_matrixfile);
  fclose(fp_matrixfile1);
  fclose(fp_vectorfile);
  fclose(fp_vectorfile1);
  fclose(fp_resultfile);
  printf("end of program\n");

  //calculate time
  gettimeofday(&tve, NULL);
  reportedTime = tve.tv_usec - tvs.tv_usec;
  //printf("COST OF MV.C \nmatrix dimension : %d\nk : %d\ntime elapsed: %d\n", matrix_dimension, k, reportedTime); 

  exit(0);
}

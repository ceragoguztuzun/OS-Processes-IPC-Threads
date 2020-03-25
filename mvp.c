#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <sys/types.h>

#define MAX_FILE_NAME_LEN 10
#define WRITE_END 1
#define READ_END 0

int main(int argc, char *argv[]) //matrixfile vectorfile resultfile k
{
 	printf ("mvp started\n"); 

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
	FILE *fp_matrixfile, *fp_matrixfile1, *fp_mapperfile, *fp_vectorfile, *fp_vectorfile1, *fp_resultfile, *k_files;
	int matrix_dimension;
	int c1;
	int intermediate_val;
	struct timeval tvs;
  	struct timeval tve;
  	unsigned long reportedTime = 0;

  	gettimeofday(&tvs, NULL); 
	//pipe variables
	int fd[2];

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

	//reducer logic
  	pid_t pid1;
  	fp_resultfile = fopen(argv[3],"w");
  	int *result_arr=(int*)calloc(matrix_dimension,sizeof(int));
  	pid = fork();
  	if(pid == 0)
  	{
  		//mappers logic
  		for (i=0; i<file_no; ++i)
  		{
  			//data generation
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

  			//pipe logic
  			pipe(fd);
  			pid1 = fork();
  			if (pid1 == 0)
  			{
  				//child process will implement write
  				close(fd[READ_END]);
  				for (c1 = 0; c1 < matrix_dimension; ++c1)
  				{
  					write(fd[WRITE_END], &midresult_arr[c1], sizeof(midresult_arr[c1]));
  				}
  				close(fd[WRITE_END]);
  				exit(0);
  			}
  			else
  			{
  				wait(NULL);
  				//parent process will implement read
  				close(fd[WRITE_END]);
  				for (c1 = 0; c1 < matrix_dimension; ++c1)
  				{
  					read(fd[READ_END], &val, sizeof(val));
  					result_arr[c1] += val;
  				}
  				close(fd[READ_END]);
  			}
  		}
  		//write array to file
  		for( i=0; i < matrix_dimension; ++i)
  		{
  			fprintf(fp_resultfile, "%d\t", i+1);
  			fprintf(fp_resultfile, "%d\n", result_arr[i]);
  		}
  	}
  	wait(NULL);
  	printf("end of program\n");
  	//calculate time
  	gettimeofday(&tve, NULL);
  	reportedTime = tve.tv_usec - tvs.tv_usec;
  	//printf("COST OF MVP.C \nmatrix dimension : %d\nk : %d\ntime elapsed: %d\n", matrix_dimension, k, reportedTime); 

 	return 0;
}

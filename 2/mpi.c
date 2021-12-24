#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <sys/time.h>
#include <time.h>
#include <math.h>
#include <limits.h>

//Allocates space for 2d array using pointers
float **alloc_2d(int rows, int cols) {
    float *data = (float *)malloc(rows*cols*sizeof(double));
    float **array= (float **)malloc(rows*sizeof(float*));
    for (int i=0; i<rows; i++)
        array[i] = &(data[cols*i]);

    return array;
}

#define swap(x, y) { float temp = x; x = y; y = temp; }

//partition for quickselect
int partition(float arr[], int l, int r)
{
    float x = arr[r];
	int	i = l;
    for (int j = l; j <= r - 1; j++) {
        if (arr[j] <= x) {
            swap(arr[i], arr[j]);
            i++;
        }
    }
    swap(arr[i], arr[r]);
    return i;
}

//quickselect
float quickselect(float arr[], int l, int r, int k)
{
    // If k is smaller than number of
    // elements in array
    if (k > 0 && k <= r - l + 1) {

        // Partition the array around last
        // element and get position of pivot
        // element in sorted array
        int index = partition(arr, l, r);

        // If position is same as k
        if (index - l == k - 1)
            return arr[index];

        // If position is more, recur
        // for left subarray
        if (index - l > k - 1)
            return quickselect(arr, l, index - 1, k);

        // Else recur for right subarray
        return quickselect(arr, index + 1, r,
                            k - index + l - 1);
    }

    // If k is more than number of
    // elements in array
    return -1;
}

//validator
void validator(float** original, int pid, int N, int d, int p, float pivot[]){
    MPI_Status status;

    //Calculates distance squared (Mr. Pitsianis said to use squared)
    float my_sum = 0;
    int i, j;
    int correct = 1;
    float dif = 0;
    float last_sum = 0;
     for(i = 0; i < N/p; i++){
       for(j = 0; j < d; j++){
         dif = pivot[j] - original[i][j];
         my_sum +=  pow(dif, 2);
         }
     }

    if(pid != (p-1)){
      //send sum
      MPI_Send((&my_sum), 1, MPI_FLOAT, pid+1, 10, MPI_COMM_WORLD);
    }

    if(pid != 0){
      //receive previous sum
      MPI_Recv((&last_sum), 1, MPI_FLOAT, pid-1, 10, MPI_COMM_WORLD, &status);
      if (pid != 1) {
        MPI_Recv((&correct), 1, MPI_INT, pid-1, 10, MPI_COMM_WORLD, &status);
      }
      if(pid != (p-1)){
        if(last_sum <= my_sum && correct){
          correct = 1;
          MPI_Send((&correct), 1, MPI_FLOAT, pid+1, 10, MPI_COMM_WORLD);
        }
        else{
          correct = 0;
          MPI_Send((&correct), 1, MPI_FLOAT, pid+1, 10, MPI_COMM_WORLD);
        }
      }
      else{
        if(last_sum <= my_sum && correct){
          printf("The trades were successful, values placed correctly!\n");
        }
        else{
          printf("The trades were  unsuccessful, values placed incorrectly!\n");
        }
      }
    }

    return;
}

float** distributeByMedian(float** original, int pid, int N, int d, int p, int first, int last, int num, float pivot[]){
  MPI_Status status;
  float* temp = (float *) malloc(d*sizeof(float));

  if(p == 1){
    printf("Ended %d \n", pid);
    return original;
  }

  if(pid == first) {
    int i, j, k;
    if(num == -1){
      //picks random pivot
      srand(time(0));
      int num = (rand() %(N/p));
      //num = 2583;

    printf("pivot is %d \n", num);

    temp = &original[num][0];

    for (i = 0; i < d; i++) {
      pivot[i] = temp[i];
    }

    //send the pivot
    k = pid + 1;
    while (k < last) {
      //printf("I %d trying to sent %d dude\n", pid, k);
      MPI_Send(temp, d, MPI_FLOAT, k, 1, MPI_COMM_WORLD);
      //printf("This is a send #1\n");
      //printf("sent %d dude\n", k);
      k++;
    }

  }
    printf("the pivot is  ");
    for (i = 0; i < d; i++) {
      printf("%f ", pivot[i]);
    }
    float distances[N];
    float val;
    float dif;
    float    my_distances[N/p];
    for(i = 0; i < N/p; i++){
      my_distances[i] = 0;
    }
    //Calculates distance squared (Mr. Pitsianis said to use squared)
    for(i = 0; i < N/p; i++){
      val = 0;
      for(j = 0; j < d; j++){
        dif = pivot[j] - original[i][j];
        //printf("process %d pivot %f my val %f dif %f \n", pid, pivot[j], original[i][j], dif);
        val +=  pow(dif, 2);
        //printf( "distance %f , val %f \n", dif, val);
        }
        my_distances[i] = val;
        distances[i] = val;
        //printf("\n %f ", my_distances[i]);
    }
    printf("\n");



    k = pid + 1;
    while (k < last) {
      float their_distances[N/p];
      //receive distances
      MPI_Recv(their_distances, N/p, MPI_FLOAT, k, 2, MPI_COMM_WORLD, &status);
      //printf("received %d dude\n", k);
      for(i = 0; i < N/p; i++){
        distances[(k-first) * N/p + i] = their_distances[i];
      }
      k++;
    }
    //printf("i'm done dude\n");

    /*for(i = 0; i < N; i++){
      printf(" %f ", distances[i]);
    }*/

    printf("\n");

    float median = (quickselect(distances, 0, N-1, N/2) + quickselect(distances, 0, N-1, N/2+1))/2;
    printf("median is %f \n", median);


    k = pid + 1;
    //send median
    while (k < last) {
      MPI_Send(&median, 1, MPI_FLOAT, k, 3, MPI_COMM_WORLD);
      //printf("sent median to %d dude\n", k);
      k++;
    }

    int smaller[N/p];
    int num_smaller = 0;
    for(i = 0; i < N/p; i++){
      if(my_distances[i] < median){
        //printf("i %d, distance %f, median %f \n", i, distances[i], median);
        smaller[i] = 1;
        num_smaller ++;
      }
      else{
        smaller[i] = 0;
      }
    }

    int big[p/2], small[p/2];
    for(i=0; i < p/2; i++){
      big[i] = -1;
      small[i] = -1;
    }
    big[0] = (N/p-num_smaller);
    k = pid + 1;
    while (k < (first+last)/2) {
      //receive how many bigger the first half have
      MPI_Recv(&big[k-first], 1, MPI_FLOAT, k, 4, MPI_COMM_WORLD, &status);
      k++;
    }
    while (k < last) {
      //receive how many smaller the second half have
      MPI_Recv(&small[k-(first+last)/2], 1, MPI_FLOAT, k, 4, MPI_COMM_WORLD, &status);
      k++;
    }
    printf("I %d, got %d smaller and %d bigger \n", pid, num_smaller, (N/p-num_smaller));

    printf("The things\n" );
    for(i = 0; i < p/2; i++){
      printf("%d ", big[i]);
    }
    for(i = 0; i < p/2; i++){
      printf("%d ", small[i]);
    }
    printf("\n");





    //data_big[0] stores how many bigger numbers to send to data_big[1], similar for data_small
    int data_big[2], data_small[2];
    for(i = 0; i < 2; i++){
      data_big[i] = 0;
      data_small[i] = 0;
    }

    /*if (pid == 2) {
       for (int i = 0; i < N/p; i++) {
         for (int j = 0; j < d; j++) {
           printf("%f ", original[i][j]);
         }
         printf("\n");
       }
     }*/

    //organise trades
    i = 0;
    j = 0;
    while(i < p/2){
      while(j < p/2){
        data_big[1] = j + (first+last)/2;
        data_small[1] = i + first;
        if(big[i] == 0){
          i++;
        }
        else if(small[i] == 0){
          j++;
        }
        else {
          if(big[i] < small[j]){
            if(pid + i == first){
              //moves all points with bigger than median distance to the front
              for(int l=0;l<N/p; l++){
                  for(int m=0;m<d;m++){
                    if(l > m && smaller[l] == 0 && smaller[m] == 1){
                      float* tmp = &(original[l][0]);
                      original[l] = &original[m][0];
                      original[m] = tmp;
                      smaller[l] = 1;
                      smaller[m] = 0;
                    }
                  }
                }
              data_small[0] = big[i];
              printf("I %d am in first \n", pid);
              printf("I %d need to send %d, to %d \n", pid, big[i], data_big[1]);
              MPI_Send(&data_small, 2, MPI_INT, data_big[1], 5, MPI_COMM_WORLD);
              printf("I %d sent data for %d, to %d \n", pid, big[i], data_big[1]);
              float **send_buffer = alloc_2d(big[i], d);
              for (int l = 0; l < big[i]; l++) {
                for (int m = 0; m < d; m++) {
                  send_buffer[l][m] = original[l][m];
                }
              }
              printf("I %d will send points \n", pid);
              MPI_Send(&(send_buffer[0][0]), big[i] * d, MPI_FLOAT, data_big[1], 6, MPI_COMM_WORLD);
              float **recv_buffer = alloc_2d(big[i], d);
              printf("I %d will receive points \n", pid);
              MPI_Recv(&(recv_buffer[0][0]), big[i] * d, MPI_FLOAT, data_big[1], 6, MPI_COMM_WORLD, &status);
              printf("I %d have received points \n", pid);
              for (int l = 0; l < big[i]; l++) {
                for (int m = 0; m < d; m++) {
                  original[l][m] = recv_buffer[l][m] ;
                }
                smaller[l] = 1;
              }
              printf("I %d sent %d, to %d \n", pid, big[i], data_big[1]);
              free(send_buffer);
              free(recv_buffer);
            }
            else{
              data_big[0] = big[i];
              data_small[0] = big[i];
              MPI_Send(&data_big, 2, MPI_INT, data_small[1], 5, MPI_COMM_WORLD);
              MPI_Send(&data_small, 2, MPI_INT, data_big[1], 5, MPI_COMM_WORLD);
            }
            printf("I am here 1");
            small[j] = small[j] - big[i];
            //big[i] = 0;
            i++;
            printf("I am here 1.1");
          }
          else if(big[i] > small[j]){
            if(pid + i == first){
              //moves all points with bigger than median distance to the front
              for(int l=0;l<N/p; l++){
                  for(int m=0;m<d;m++){
                    if(l > m && smaller[l] == 0 && smaller[m] == 1){
                      float* tmp = &(original[l][0]);
                      original[l] = &original[m][0];
                      original[m] = tmp;
                      smaller[l] = 1;
                      smaller[m] = 0;
                    }
                  }
                }
              data_small[0] = small[j];
              printf("I %d am in second \n", pid);
              printf("I %d need to send %d, to %d \n", pid, small[j], data_big[1]);
              MPI_Send(&data_small, 2, MPI_INT, j + (first+last)/2, 5, MPI_COMM_WORLD);
              printf("I %d sent data for %d, to %d \n", pid, small[j], data_big[1]);
              float **send_buffer = alloc_2d(big[i], d);
              for (int l = 0; l < small[j]; l++) {
                for (int m = 0; m < d; m++) {
                  send_buffer[l][m] = original[l][m];
                }
              }
              printf("I %d will send points \n", pid);
              MPI_Send(&(send_buffer[0][0]), small[j] * d, MPI_FLOAT, data_big[1], 6, MPI_COMM_WORLD);
              printf("I %d sent points \n", pid);
              float **recv_buffer = alloc_2d(big[i], d);
              printf("I %d will receive points \n", pid);
              MPI_Recv(&(recv_buffer[0][0]), small[j] * d, MPI_FLOAT, data_big[1], 6, MPI_COMM_WORLD, &status);
              printf("I %d have received points \n", pid);
              for (int l = 0; l < small[j]; l++) {
                for (int m = 0; m < d; m++) {
                  original[l][m] = recv_buffer[l][m] ;
                }
                smaller[l] = 1;
              }
            }
            else{
              data_big[0] = small[j];
              data_small[0] = small[j];
              MPI_Send(&data_big, 2, MPI_INT, data_small[1], 5, MPI_COMM_WORLD);
              MPI_Send(&data_small, 2, MPI_INT, data_big[1], 5, MPI_COMM_WORLD);
            }
            printf("here here here\n");
            big[i] = big[i] - small[j];
            //small[j] = 0;
            j++;
          }
          else if (big[i] == small[j]){
            if(pid + i == first){
              //moves all points with bigger than median distance to the front
              for(int l=0;l<N/p; l++){
                  for(int m=0;m<d;m++){
                    if(l > m && smaller[l] == 0 && smaller[m] == 1){
                      float* tmp = &(original[l][0]);
                      original[l] = &original[m][0];
                      original[m] = tmp;
                      smaller[l] = 1;
                      smaller[m] = 0;
                    }
                  }
                }
              data_small[0] = big[i];
              printf("I %d am in third \n", pid);
              printf("I %d need to send %d, to %d \n", pid, big[i], data_big[1]);
              MPI_Send(&data_small, 2, MPI_INT, j + (first+last)/2, 5, MPI_COMM_WORLD);
              printf("I %d sent data for %d, to %d \n", pid, big[i], data_big[1]);
              float **send_buffer = alloc_2d(big[i], d);
              for (int l = 0; l < big[i]; l++) {
                for (int m = 0; m < d; m++) {
                  send_buffer[l][m] = original[l][m];
                }
              }
              printf("I %d will send points \n", pid);
              MPI_Send(&(send_buffer[0][0]), big[i] * d, MPI_FLOAT, data_big[1], 6, MPI_COMM_WORLD);
              printf("I %d sent points \n", pid);
              float **recv_buffer = alloc_2d(big[i], d);
              printf("I %d will receive points \n", pid);
              MPI_Recv(&(recv_buffer[0][0]), big[i] * d, MPI_FLOAT, data_big[1], 6, MPI_COMM_WORLD, &status);
              printf("I %d have received points \n", pid);
              for (int l = 0; l < big[i]; l++) {
                for (int m = 0; m < d; m++) {
                  original[l][m] = recv_buffer[l][m] ;
                }
                smaller[l] = 1;
              }
              printf("I %d sent %d, to %d \n", pid, big[i], data_big[1]);
              free(send_buffer);
              free(recv_buffer);
            }
            else{
              data_big[0] = big[i];
              data_small[0] = big[i];
              MPI_Send(&data_big, 2, MPI_INT, data_small[1], 5, MPI_COMM_WORLD);
              MPI_Send(&data_small, 2, MPI_INT, data_big[1], 5, MPI_COMM_WORLD);
            }
            printf("I am here 3");
            //big[i] = 0;
            //small[j] = 0;
            i++;
            j++;
            printf("I am here 3.3");
          }
        }
      }
    }
    /*if (pid == 2) {
       for (int i = 0; i < N/p; i++) {
         for (int j = 0; j < d; j++) {
           printf("%f ", original[i][j]);
         }
         printf("\n");
       }
     }*/



  }

  else if(pid > first && pid < last) {
  //  printf("I started %d\n", pid );
    if(num == -1){
      //receive the pivot
      MPI_Recv(temp, d, MPI_FLOAT, first, 1, MPI_COMM_WORLD, &status);
    //  printf("This is receive 1\n");
      //printf("got it mate %d\n", pid );
      for(int i = 0; i < d; i++) {
        pivot[i] = temp[i];
      }
      free(temp);
    }
    /*printf("pivot is ");
    for(int i = 0; i < d; i++){
      printf("%f ", pivot[i]);
    }
      printf("\n");*/

    //Calculates distance squared (Mr. Pitsianis said to use squared)
    float my_distances[N/p];
    int i, j;
    float dif = 0;
    float val;
    for(i = 0; i < N/p; i++){
      my_distances[N/p] = 0;
    }
     for(i = 0; i < N/p; i++){
       val = 0;
       for(j = 0; j < d; j++){
         dif = pivot[j] - original[i][j];
         //printf("process %d pivot %f my val %f dif %f ", pid, pivot[j], original[i][j], dif);
         val +=  pow(dif, 2);
        // printf( "distance %f , val %f \n", dif, val);
         }
         my_distances[i] = val;
        //printf("\n %f ", my_distances[i]);
     }
     for(i = 0; i < N/p; i++){
      // printf("\n %f ", my_distances[i]);
     }
    // printf("calculated it mate %d\n ~~~~~~~~~~~~~~~~~~~~~~ \n", pid );
     //send distances
     MPI_Send((&my_distances), N/p, MPI_FLOAT, first, 2, MPI_COMM_WORLD);
    // printf("sent my distances\n");
     float median;
     MPI_Recv(&median, 1, MPI_FLOAT, first, 3, MPI_COMM_WORLD, &status);
     //printf("process %d got median %f \n", pid, median);

     int smaller[N/p];
     int num_smaller = 0;
     for(i = 0; i < N/p; i++){
       if(my_distances[i] <= median){
         smaller[i] = 1;
         num_smaller ++;
       }
       else{
         smaller[i] = 0;
       }
     }
     int num_bigger = (N/p-num_smaller);


    //sends information about the number of mismatched data
    if(pid >= first && pid < (last+first)/2){
      MPI_Send(&num_bigger, 1, MPI_FLOAT, first, 4, MPI_COMM_WORLD);
     }
    if(pid >= (first+last)/2  && pid < last){
      MPI_Send(&num_smaller, 1, MPI_FLOAT, first, 4, MPI_COMM_WORLD);
     }
     printf("I %d, got %d smaller and %d bigger \n", pid, num_smaller, num_bigger);

    /*if (pid == 3) {
       for (int i = 0; i < N/p; i++) {
         for (int j = 0; j < d; j++) {
           printf("%f ", original[i][j]);
         }
         printf("\n");
       }
     }*/

     //receives trade information
     int data[i];
     if(pid >= first && pid < (last+first)/2){
       while(num_bigger > 0){



         //moves all point with bigger than median distance to the front
         for(i=0;i<N/p; i++){
             for(j=0;j<N/p;j++){
               if(i > j && smaller[i] == 0 && smaller[j] == 1){
                 float* tmp = &original[i][0];
                 original[i] = &original[j][0];
                 original[j] = tmp;
                 smaller[i] = 1;
                 smaller[j] = 0;
               }
             }
           }
           printf("I %d waiting for data\n ", pid);
          MPI_Recv(&data, 2, MPI_INT, first, 5, MPI_COMM_WORLD, &status);
          printf("I %d need to send %d, to %d ", pid, data[0], data[1]);
          float **send_buffer = alloc_2d(data[0], d);
          for (int l = 0; l < data[0]; l++) {
            for (int m = 0; m < d; m++) {
              send_buffer[l][m] = original[l][m];
            }
          }
          MPI_Send(&(send_buffer[0][0]), data[0] * d, MPI_FLOAT, data[1], 6, MPI_COMM_WORLD);
          float **recv_buffer = alloc_2d(data[0], d);
          MPI_Recv(&(recv_buffer[0][0]), data[0] * d, MPI_FLOAT, data[1], 6, MPI_COMM_WORLD, &status);
          for (int l = 0; l < data[0]; l++) {
            for (int m = 0; m < d; m++) {
              original[l][m] = recv_buffer[l][m];
            }
            smaller[l] = 1;
          }
         num_bigger -= data[0];
         printf("num_bigger is %d \n", num_bigger);
         free(send_buffer);
         free(recv_buffer);
       }
      }
     if(pid >= (first+last)/2  && pid < last){
       while(num_smaller > 0){

         //moves all point with smaller than median distance to the front
         for(i=0;i<N/p; i++){
             for(j=0;j<N/p;j++){
               if(i < j && smaller[i] == 0 && smaller[j] == 1){
                 float* tmp = &original[i][0];
                 original[i] = &original[j][0];
                 original[j] = tmp;
                 smaller[i] = 1;
                 smaller[j] = 0;
               }
             }
           }

           printf("I %d waiting for data\n ", pid);
         MPI_Recv(&data, 2, MPI_INT, first, 5, MPI_COMM_WORLD, &status);
         printf("I %d need to send %d, to %d ", pid, data[0], data[1]);
         float **recv_buffer = alloc_2d(data[0], d);
         float **send_buffer = alloc_2d(data[0], d);
         for (int l = 0; l < data[0]; l++) {
           for (int m = 0; m < d; m++) {
             send_buffer[l][m] = original[l][m];
           }
         }
         MPI_Recv(&(recv_buffer[0][0]), data[0] * d, MPI_FLOAT, data[1], 6, MPI_COMM_WORLD, &status);
         for (int l = 0; l < data[0]; l++) {
           for (int m = 0; m < d; m++) {
             original[l][m] = recv_buffer[l][m] ;
           }
           smaller[l] = 0;
         }

         MPI_Send(&(send_buffer[0][0]), data[0] * d, MPI_FLOAT, data[1], 6, MPI_COMM_WORLD);

         num_smaller -= data[0];
         printf("num_smaller is %d \n", num_smaller);
         free(send_buffer);
         free(recv_buffer);
       }
    }
    /*if (pid == 3) {
      for (int i = 0; i < N/p; i++) {
        for (int j = 0; j < d; j++) {
          printf("%f ", original[i][j]);
        }
        printf("\n");
      }
    }*/
    //  printf("I 2nd %d, got %d smaller and %d bigger \n", pid, num_smaller, num_bigger);
  }
  num++;
  printf("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~` \n I %d finished call %d \n ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ \n", pid, num);
  MPI_Barrier(MPI_COMM_WORLD);
  if(pid >= first && pid < (last+first)/2){
    distributeByMedian(original, pid, N/2, d, p/2, first, (first+last)/2, num, pivot);
  }
  if(pid >= (first+last)/2  && pid < last){
    distributeByMedian(original, pid, N/2, d, p/2, (first+last)/2, last, num, pivot);
  }
}


int main(int argc, char** argv){
  int pid;
  int p;
  int i,j;
  int N, d;
  struct timeval startwtime, endwtime;
  double seq_time;
  //int N = 16777216;
  //int d = 32;
  N = atoi(argv[1]);
	d = atoi(argv[2]);
//  N = 16;
//  d = 4;
  float **original;

  float pivot[d];




  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &pid);
  MPI_Comm_size(MPI_COMM_WORLD, &p);

  FILE *file = fopen(argv[3], "r");

  original = (float **) alloc_2d(N/p, d);

  int count = 0;
  char line[1000*d];
       while (fgets(line, sizeof line, file) != NULL) /* read a line */
     {
         if (count == pid * N/p)
         {
             //printf("I am %d and I have %s", pid, line);
             original[0][0] = atof(strtok(line, ","));
             //printf("%f ", original[0][0]);
             for (j = 1; j < d; j++) {
               original[0][j] = atof(strtok(NULL, ","));
               //printf("%f ", original[0][j]);
             }
             //printf("\n");
             for (i = 1; i < N/p; i++) {
               fgets(line, sizeof line, file);
               //printf("I am %d and I have %s", pid, line);
               original[i][0] = atof(strtok(line, ","));
            // printf("%f ", original[i][0]);
               for (j = 1; j < d; j++) {
                 original[i][j] = atof(strtok(NULL, ","));
                // printf("%f ", original[i][j]);
               }
               //printf("\n");
             }
             break;
         }
         else
         {
             count++;
         }
     }
     fclose(file);

  float** final = (float **) alloc_2d(N/p, d);

  MPI_Barrier(MPI_COMM_WORLD);
  if(pid == 0){
    gettimeofday(&startwtime, NULL);
  }

  final = distributeByMedian(original, pid, N, d, p, 0, p, -1, pivot);

  MPI_Barrier(MPI_COMM_WORLD);
  if(pid == 0){
    gettimeofday(&endwtime, NULL);
    seq_time = (double)((endwtime.tv_usec - startwtime.tv_usec)/1.0e6
  		      + endwtime.tv_sec - startwtime.tv_sec);
    printf("Finished in %f seconds\n", seq_time);
  }

  validator(final, pid, N, d, p, pivot);

  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();


  return(0);
}

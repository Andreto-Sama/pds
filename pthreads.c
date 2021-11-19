#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <sys/time.h>
#include "mmio.h"

pthread_mutex_t lock;

void coo2csc(
  uint32_t       * const row,       /*!< CSC row start indices */
  uint32_t       * const col,       /*!< CSC column indices */
  uint32_t const * const row_coo,   /*!< COO row indices */
  uint32_t const * const col_coo,   /*!< COO column indices */
  uint32_t const         nnz,       /*!< Number of nonzero elements */
  uint32_t const         n,         /*!< Number of rows/columns */
  uint32_t const         isOneBased /*!< Whether COO is 0- or 1-based */
) {

  // ----- cannot assume that input is already 0!
  for (uint32_t l = 0; l < n+1; l++) col[l] = 0;


  // ----- find the correct column sizes
  for (uint32_t l = 0; l < nnz; l++)
    col[col_coo[l] - isOneBased]++;

  // ----- cumulative sum
  for (uint32_t i = 0, cumsum = 0; i < n; i++) {
    uint32_t temp = col[i];
    col[i] = cumsum;
    cumsum += temp;
  }
  col[n] = nnz;
  // ----- copy the row indices to the correct place
  for (uint32_t l = 0; l < nnz; l++) {
    uint32_t col_l;
    col_l = col_coo[l] - isOneBased;

    uint32_t dst = col[col_l];
    row[dst] = row_coo[l] - isOneBased;

    col[col_l]++;
  }
  // ----- revert the column pointers
  for (uint32_t i = 0, last = 0; i < n; i++) {
    uint32_t temp = col[i];
    col[i] = last;
    last = temp;
  }

}

typedef struct {
  uint32_t i, N, thr;
  uint32_t * val;
  uint32_t * csc_row;
  uint32_t * csc_col;
} parm;

void *compute(void *arg) {
  parm *p = (parm *)arg;
  uint32_t  j, k, l, m, start, stop;
  uint32_t  i = p->i, N = p->N, thr = p->thr;
  start = i * N/thr;
  if(i == thr-1){
    stop = N;
  }
  else{
    stop = (i + 1) * N/thr;
  }

  for(m=start; m<stop;m++){
    for(j=p->csc_col[m]; j < p->csc_col[m+1]; j++){
      for(k=p->csc_col[p->csc_row[j]]; k<p->csc_col[p->csc_row[j]+1]; k++){
        for(l=p->csc_col[m]; l < p->csc_col[m+1]; l++){
          if(p->csc_row[k] == p->csc_row[l]){
            pthread_mutex_lock(&lock);
            p->val[j]++;
            pthread_mutex_unlock(&lock);
          }
        }
      }
    }
  }

  pthread_exit(NULL);
}

int main(int argc, char *argv[])
{
    uint32_t ret_code;
    MM_typecode matcode;
    FILE *f;
    uint32_t M, N, nz;
    uint32_t i, j, k, l, triangles;
    uint32_t * I;
    uint32_t * J;
    struct timeval startwtime, endwtime;
    double seq_time;

    if (argc < 2)
	{
		fprintf(stderr, "Usage: %s [martix-market-filename]\n", argv[0]);
		exit(1);
	}
    else
    {
        if ((f = fopen(argv[1], "r")) == NULL)
            exit(1);
    }

    if (mm_read_banner(f, &matcode) != 0)
    {
        printf("Could not process Matrix Market banner.\n");
        exit(1);
    }


    /*  This is how one can screen matrix types if their application */
    /*  only supports a subset of the Matrix Market data types.      */

    if (mm_is_complex(matcode) && mm_is_matrix(matcode) &&
            mm_is_sparse(matcode) )
    {
        printf("Sorry, this application does not support ");
        printf("Market Market type: [%s]\n", mm_typecode_to_str(matcode));
        exit(1);
    }

    /* find out size of sparse matrix .... */

    if ((ret_code = mm_read_mtx_crd_size(f, &M, &N, &nz)) !=0)
        exit(1);


    /* reseve memory for matrices */

    I = (uint32_t *) malloc(2 * nz * sizeof(uint32_t));
    J = (uint32_t *) malloc(2 * nz * sizeof(uint32_t));


    /* NOTE: when reading in doubles, ANSI C requires the use of the "l"  */
    /*   specifier as in "%lg", "%lf", "%le", otherwise errors will occur */
    /*  (ANSI C X3.159-1989, Sec. 4.9.6.2, p. 136 lines 13-15)            */

    for (i=0; i<2*nz; i+= 2)
    {
        fscanf(f, "%d %d \n", &I[i], &J[i]);
        I[i]--;  /* adjust from 1-based to 0-based */
        J[i]--;
        I[i+1] = J[i];
        J[i+1] = I[i];
    }

    if (f !=stdin) fclose(f);

    uint32_t * csc_row = (uint32_t *)malloc(2 * nz * sizeof(uint32_t));
    uint32_t * csc_col = (uint32_t *)malloc((N + 1) * sizeof(uint32_t));
    uint32_t isOneBased = 0;

    coo2csc(csc_row, csc_col, I, J, 2 * nz, N,isOneBased);
  //All the above just reads the mtx file and transforms it to csc format
  gettimeofday (&startwtime, NULL);
  uint32_t thr = atoi(argv[2]);
  uint32_t cr_thr;
  pthread_t *threads;
  parm *p;

  threads = (pthread_t *)malloc(thr*sizeof(pthread_t));

  p = (parm *)malloc(sizeof(parm)*thr);
  //Store the result
  uint32_t * val = (uint32_t *)malloc(2 * nz * sizeof(uint32_t));
  for(i=0; i< 2*nz;i++){
    val[i]=0;
  }

  for(i=0; i<thr;i++){
    p[i].i = i;
    p[i].N = N;
    p[i].thr = thr;
    p[i].csc_row = csc_row;
    p[i].csc_col = csc_col;
    p[i].val = val;
    pthread_create(&threads[i], NULL,  compute, (void *)(p+i));
  }

    for (i = 0; i<thr; i++) {
      pthread_join(threads[i],NULL);
    }

    free(p);

  for(i=0; i< 2*nz;i++){
    triangles += val[i];
  }
  triangles = triangles/6;
  printf("%d\n", triangles);
  gettimeofday (&endwtime, NULL);
  seq_time = (double)((endwtime.tv_usec - startwtime.tv_usec)/1.0e6
		      + endwtime.tv_sec - startwtime.tv_sec);
  printf("Finished in %f seconds\n", seq_time);
	return 0;
}

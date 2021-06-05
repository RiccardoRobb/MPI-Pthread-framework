#include "core.c"

int addPendingActions(pendingActions ** pa, int * pa_size, int action, int numWorkers, listWorkers * workers);
void printPendingActions(pendingActions * pa, int pa_size);
void printMatrix(int * matrix, int rows, int cols);
void printMatrices(matrix * matrices, int lenMatrices);
int getFreeThreads(threadStatus * status, int commSize, int numThreads);
int getIntFromStdin(int * dest);
void printActions(int * actions);
int popAction(int * actions, int * dest);
int pushAction(int * actions, int action);
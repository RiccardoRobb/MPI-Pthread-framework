
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include "/usr/include/x86_64-linux-gnu/mpich/mpi.h"
#include <stdlib.h>
#include <stdbool.h>

// Size massima di azioni che possono essere accodate
#define SIZE 10
// Valore massimo generabile dalla chiamata di rand()
#define MAX_RAND 536870911

pthread_mutex_t stdoutMutex = PTHREAD_MUTEX_INITIALIZER;


/* Contiene informazioni relative ai workers partecipanti all'esecuzione di un determinata azione presente in pendingActions 
 * worker   rank del processo interessato
 * size     numero di elementi che mi aspetto dal determinato worker
 * status   identificativo se la richiesta è stata completata cosi evito di ricontrollarla
 * request  richiesta creata in seguito all'utilizzo di una funzione non bloccante */ 
typedef struct
{
    int rank;
    int size;
    bool status;
    MPI_Request request;
} listWorkers;

/* Conservo informazioni relative ad un action in attesa di essere completata 
 * action       identificativo dell'azione
 * numWorkers   numero di processi partecipanti
 * completed    uguale a true se tutte le richieste dei workers partecipanti sono state testate e risultano essere completate
 * publish      se il risultato dell'azione è stato stampato a video
 * workers      lista dei workers partecipanti all'esecuzione della azione
 * startTime    tempo di inizio dell'esecuzione */
typedef struct {
    int action;
    int numWorkers;
    bool completed;
    bool publish;
    listWorkers * workers;
    double startTime;
} pendingActions;

// Contiene informazioni relative allo stato dei threads dei workers
typedef struct {
    int rank;
    int * threads;
} threadStatus;

// Conserva le informazioni riguardanti le matrici create
typedef struct {
    int numRows;
    int numCols;
    ulong size;
    int * pointerMatrix;
} matrix;

typedef struct {
    int action;
    int size;
    int * matrix;
    int numThreads;
    int * threads;
    bool completed;
    bool send;
    MPI_Request request;
} work;

int addWork(work ** works, int * works_size, int action, int size, int * matrix, int numThreads, int * threads)
{
    for(int i = 0; i < *works_size; i++)
    {
        if ((*works)[i].completed)
        {
            free((*works)[i].threads);
            (*works)[i].action = action;
            (*works)[i].size = size;
            (*works)[i].matrix = matrix;
            (*works)[i].numThreads = numThreads;
            (*works)[i].threads = threads;
            (*works)[i].completed = false;
            (*works)[i].send = false;
            return 0;
        }
    }
    (*works_size)++;
    *works = realloc(*works, sizeof(work) * (*works_size));
    (*works)[(*works_size) - 1].action = action;
    (*works)[(*works_size) - 1].size = size;
    (*works)[(*works_size) - 1].matrix = matrix;
    (*works)[(*works_size) - 1].numThreads = numThreads;
    (*works)[(*works_size) - 1].threads = threads;
    (*works)[(*works_size) - 1].completed = false;
    (*works)[(*works_size) - 1].send = false;
    return 0;
}


// Ritorno il numero di threads totali liberi dei workers
int getFreeThreads(threadStatus * status, int commSize, int numThreads)
{
    int freeThreads = 0;
    for(int i = 0; i < commSize - 1; i++)
        for(int j = 0; j < numThreads; j++)
            if (!(status[i].threads[j])) freeThreads++;
    return freeThreads;
}

// Stampa a video il resoconto delle matrici indicate e quindi i dati immessi in input in fase di creazione
void printMatrices(matrix * matrices, int lenMatrices)
{
    printf("RESOCONTO DELLE MATRICI CREATE\n");
    for(int i = 0; i < lenMatrices; i++)
    {
        printf("%d-esima matrice:\n", i);
        printf("Numero di righe: %d\n", matrices[i].numRows);
        printf("Numero di colonne: %d\n", matrices[i].numCols);
        printf("Numero di elementi da cui è composta la matrice: %lu\n", matrices[i].size);
        printf("Puntatore alla matrice: %p\n\n", matrices[i].pointerMatrix);
    }
}

// Stampa a video della matrice passata come argomento formattata in base alle righe e alle colonne
void printMatrix(int * matrix, int rows, int cols)
{
    for(int y=0; y<rows; y++)
    {
        for(int x=0; x<cols; x++) printf("%d ", *(matrix + (cols*y) + x));
        printf("\n");
    }
}

// Stampa a video delle azioni in esecuzione in attesa di essere completate
void printPendingActions(pendingActions * pa, int pa_size)
{
    bool empty = true;

    for(int i = 0; i < pa_size; i++)
    {
        if (pa[i].workers)
        {
            empty = false;
            printf("Alla posizione %d --- Azione: %d numWorkers: %d completed: %d published: %d\n", i, pa[i].action, pa[i].numWorkers, pa[i].completed, pa[i].publish);
            for(int j = 0; j < pa[i].numWorkers; j++)
                printf("Worker n: %d Rank: %d Size: %d Status: %d\n", j, pa[i].workers[j].rank, pa[i].workers[j].size, pa[i].workers[j].status);
        }
    }

    if (empty) {
        pthread_mutex_lock(&stdoutMutex);
        printf("Non ci sono azioni programmate in esecuzione\n");
        pthread_mutex_unlock(&stdoutMutex);
    } 
}

/* Aggiunge alla lista di strutture pendingActions una nuova azione da eseguire, se ho un nella lista una azione 
 * già completata allora sostituirò i dati di tali azione con la nuova azione, altrimenti riallocherò la lista 
 * aumentandone le dimensione */
int addPendingActions(pendingActions ** pa, int * pa_size, int action, int numWorkers, listWorkers * workers)
{
    for(int i = 0; i < *pa_size; i++)
    {
        if((*pa)[i].completed && (*pa)[i].publish)
        {
            (*pa)[i].action = action;
            (*pa)[i].numWorkers = numWorkers;
            (*pa)[i].completed = false;
            (*pa)[i].publish = false;
            (*pa)[i].workers = workers;
            (*pa)[i].startTime = MPI_Wtime();
            return 0;
        }
    }
    (*pa_size)++;
    *pa = realloc(*pa, sizeof(pendingActions) * (*pa_size));
    (*pa)[(*pa_size)-1].action = action;
    (*pa)[(*pa_size)-1].numWorkers = numWorkers;
    (*pa)[(*pa_size)-1].completed = false;
    (*pa)[(*pa_size)-1].publish = false;
    (*pa)[(*pa_size)-1].workers = workers;
    (*pa)[(*pa_size)-1].startTime = MPI_Wtime();
    return 0;
}

/* Funzione utilizzata per ottenere input da tastiera, essa ci permette di diagnosticare situazioni irrisolvibili durante
 * l'acquisizione dell input es. numero inserito da tastiera è troppo grande per essere memorizzato in un int */
int getIntFromStdin(int * dest)
{
    int value;
    int result = scanf("%d", &value);

    if (result == EOF) {
        printf("Errore IRRISOLVIBILE rilevato durante l'acquisizione dell'input: %s\n", strerror(errno));
        return 1;
    }

    // Se non ottengo in input un int oppure il valore è 0 (non permesso) consumo lo stream sull'stdin e richiamo la medesima funzione 
    if (result == 0 || value == 0) {
        while(fgetc(stdin) != '\n');
        return getIntFromStdin(dest);
    }
        
    *dest = value;
    return 0;
}

// Stampa a video le azioni accodate
void printActions(int * actions)
{
    for(int i = 0; i < SIZE && actions[i] != 0; i++) printf("Valore %d alla posizione %d-esima\n", actions[i], i);
}

/* Estraggo una azione da actions e setta il valore della variabile dest con il valore identificativo dell'azione, 
 * restituisco 0 se sono riuscito ad estrarre un elemento, 1 altrimenti */
int popAction(int * actions, int * dest)
{
    int i = 0;
    if(actions[0] != 0)
    {
        *dest = actions[0];
        for(i = 0; i < SIZE - 1 && actions[i + 1] != 0; i++) actions[i] = actions[i + 1];
        actions[i] = 0;
        return 0;
    }
    return 1;
}

// Accodo una azione da svolgere all'array actions (se non è stata superata la SIZE massima)
int pushAction(int * actions, int action)
{
    int i = 0;
    for(i = 0; i < SIZE && actions[i] != 0; i++);

    if (i != SIZE) {
        actions[i] = action;
        return 0;
    }
    
    return 1;
}
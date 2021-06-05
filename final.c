/* Progetto 10
 * Framework Master/Worker multithreading usando MPI/Pthread
 * Autori: Simone Ruberto e Riccardo Ruberto
 */

#include <pthread.h>
#include <unistd.h> //sleep
#include <time.h>
#include "core.h"

// Utilizzata sia dal master che dai workers per tenere il numero di threads utilizzabili da ciascun worker
int numThreads;
// Viene cambiato il valore a true quando viene richiesta la terminazione del programma
bool exit_ = false;
// Detiene l'identificativo del task da eseguire
int action;
/* Quando si vuole eseguire un task che richiede threads per essere eseguito viene temporaneamente tenuto 
 * in essa il valore identificativo del task perchè il valore di action verrà sostituito dall'identificativo 
 * del task 1 che equivale all'azione di ottenere lo stato aggiornato dei workers e quindi dei realtivi threads */
int tempAction = 0;
/* Utilizzato per garantire un accesso mutuamente esclusivo all'array contenente le task prese in input 
in attesa di essere instradate nell'infrastruttura di MPI */
pthread_mutex_t actionsMutex = PTHREAD_MUTEX_INITIALIZER;

// Utilizzata per salvare la richiesta della comunicazioni non bloccanti, essa sarà testata attraverso la funzione MPI_Test 
MPI_Request tempRequest;
// Se true significa che posso mandare in esecuzione un'altra task programmata
int nextActionLock = true;
/* Inizializzato globalmente per consentire alle funzioni che saranno eseguite dai threads dei workers 
 * di cambiare il loro stato e quindi segnalare se posso essere utilizzati o se sono già impegnati */
int * threadStatusSlaveProcess;

// Variabili di appoggio utilizzate nelle varie funzioni dai processi
int tempNumElements;
unsigned short extraElements;
int temp;
int tempS;
int * tempMatrix;
int * tempMatrixS;
bool completed = false;
int flag;

/* Usata per consentire la comunicazione tra il master e i workers
 * dove il membro numThreads identifica il numero di threads che dovrà dedicare all'esecuzione di tale azione
 * mentre size identifica il carico del lavoro */
typedef struct
{
    int numThreads;
    int size;
} msgToWorker;

/* Visto che la struttura descritta in precedenza dovrà essere trasmessa ad altri processi 
 * abbiamo dovuto definire un nuovo tipo di dato supportato da MPI */
MPI_Datatype MPI_MSG_TO_WORKER;
msgToWorker * argsToWorker;

/* Utilizzata per trasmettere informazioni a threads che dovranno essere impegnati nell'esecuzione di un task
 * tid              identificativo del thread può assumere un valore da 1 a numThreads - 1
 * pid              identificativo del processo a cui appartiene === rank
 * size             carico di lavoro
 * matrix           puntatore alla posizione in cui effettuare il lavoro 
 * displacements    dislocamento rispetto alla posizione di partenza */
typedef struct 
{
    int tid;
    int pid;
    int size;
    int * matrix;
    int displacements;
} msgToThread;

/* Eseguita dai threads inattivi di ciascun worker */
void helloWorld(msgToThread * msg)
{
    // Cambia lo stato da 0 a 1 per segnalare di essere occupato
    threadStatusSlaveProcess[msg->tid] = 1;
    //if (msg->pid % 2 == 0 && msg->tid % 2 == 0) sleep(60);
    printf("Hello world! Sono il thead %d del processo %d\n", msg->tid, msg->pid);
    threadStatusSlaveProcess[msg->tid] = 0;
}
// Eseguita dai threads per assegnare valori alla sezione di una matrice in modo randomico
void generateMatrix(msgToThread * msg)
{
    threadStatusSlaveProcess[msg->tid] = 1;
    for(int i = 0; i < msg->size; i++) 
        msg->matrix[i] = rand() % MAX_RAND;
    threadStatusSlaveProcess[msg->tid] = 0;
}
// Eseguita dai threads per effettuare la somma di due matrici 
void sumMatrix(msgToThread * msg)
{
    threadStatusSlaveProcess[msg->tid] = 1;
    for(int i = 0; i < msg->size; i++) 
        tempMatrix[msg->displacements + i] = tempMatrix[msg->displacements + i] + tempMatrixS[msg->displacements + i];
    threadStatusSlaveProcess[msg->tid] = 0;
}
/* Funzione eseguita dallo slave thread del master process, essa ci consente di poter prendere in input
 * task da dover eseguire senza richiedere l'intervento del main thread 
 * actionsToDo  puntatore all'array in cui immagazzinare gli input */
void handlerAction(int * actionsToDo)
{
    int * head = actionsToDo;
    int action;

    while (!exit_)
    {
        usleep(5000);
        /* Mostra il menù solo se il main thread non è in attesa di avere threads liberi per poter eseguire una azione */
        if (tempAction == 0)
        {          

            printf("\n\nScegli l'azione da eseguire\n");
            printf("-------------------------------------------------\n");
            printf("6 - Somma tra matrici\n");
            printf("5 - Stampa matrice\n");
            printf("4 - Genera matrice di n elementi casuali\n");
            printf("3 - Hello world da tutti i threads inattivi\n");
            printf("2 - Tasks in esecuzione con relativo stato\n");
            printf("1 - Ottieni lo stato dei workers\n");
            printf("-1 - Richiedi terminazione del programma\n");
            printf("-2 - Forza terminazione del programma\n\n");

            getIntFromStdin(&action);
        }
        
        pthread_mutex_lock(&actionsMutex);
        if (pushAction(head, action)) printf("Non e' possibile accodare altri task da eseguire, attendere lo svolgimento di quelli precedentemente programmati\n");
        pthread_mutex_unlock(&actionsMutex);
    }
}

int main()
{
    int rank;
    int commSize;

    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &commSize);

    if (commSize < 2)
    {
        printf("Il numero di processi deve essere almeno pari a 2.");
        MPI_Abort(MPI_COMM_WORLD, 0);
    }

    // Setto seed di generazione di interi così rand non dovrebbe generare gli stessi numeri
    srand(time(NULL));

    // Argomenti per generare un nuovo tipo di dato supportato da MPI
    int lengths[2] = { 1, 1 };
    const MPI_Aint displacements[2] = { 0, sizeof(int) };
    MPI_Datatype types[2] = { MPI_INT, MPI_INT };
    MPI_Type_create_struct(2, lengths, displacements, types, &MPI_MSG_TO_WORKER);
    MPI_Type_commit(&MPI_MSG_TO_WORKER);

    if(rank == 0)
    {
        printf("Inserisci il numero di thread che dovra' avere ogni worker\n");
        getIntFromStdin(&numThreads);

        if (numThreads < 2)
        {
            printf("Il numero di thread deve essere almeno pari a 2.");
            MPI_Abort(MPI_COMM_WORLD, 0);
        }

        // Trasmetto sull'infrastruttura il numero di threads utilizzabili da ciascun processo
        MPI_Bcast(&numThreads, 1, MPI_INT, 0, MPI_COMM_WORLD);
        
        pthread_t slaveThreadMainProcess;
        // Array in cui verranno accodate le azioni nel momento in cui il main thread del master è occupato
        int * actionsToDo = malloc(sizeof(int) * SIZE);
        for(int i = 0; i < SIZE; i++) actionsToDo[i] = 0;

        // Variabile utilizzata per immagazzinare il valore di ritorno della funzione popAction
        bool result = true;

        // Array di struttre in cui verrà salvato lo stato di ciascuna azione in esecuzione
        pendingActions * pa = malloc(sizeof(pendingActions));
        int pa_size = 1;
        pa->completed = true;
        pa->publish = true;
        pa->workers = NULL;
        /* Ogni azione che verrà eseguita verrà aggiunta alla struttura pendingAction e in essa sarà presente anche 
         * una lista di worker partecipanti che ne identifica lo status di completamento e i risultati attesi */
        listWorkers * tempListWorker = malloc(sizeof(listWorkers));

        // Struttura in cui salvo lo status di tutti i worker e dei relativi threads
        threadStatus * status = malloc(sizeof(threadStatus) * commSize - 1);
        for(int i = 0; i < commSize - 1; i++) status[i].threads = malloc(sizeof(int) * numThreads);

        // Variabili per salvare l'esito e lo stato della funzione MPI_Test
        MPI_Status statusTest;

        // Variabili temporanee
        int tempFreeThreads;
        bool actionWithWorkers = false;

        // Puntatore all'array di strutture in cui salvare tutte le matrici create e sui cui sono state effettuate operazioni
        matrix * matrices;

        //Variabili ausiliari
        int threadsUsed;
        matrix * tempMatrix;
        int matricesLen = 0;
        bool adjust = false;
        bool tempFree = false;

        // Puntatori a array utilizzati dalle funzioni MPI_Scatterv e MPI_Gatherv
        int * counts = calloc(sizeof(int), commSize);
        int * displacements = calloc(sizeof(int), commSize);

        double startTime, endTime;

        // Array di strutture usato per trasmettere tramite MPI il lavoro da effettuare ai workers
        argsToWorker = calloc(sizeof(msgToWorker), commSize - 1);

        printf("\n ______ _____            __  __ ________          ______  _____  _  __ \n");       
        printf("|  ____|  __ \\     /\\   |  \\/  |  ____\\ \\        / / __ \\|  __ \\| |/ / \n");       
        printf("| |__  | |__) |   /  \\  | \\  / | |__   \\ \\  /\\  / / |  | | |__) | ' /  \n");       
        printf("|  __| |  _  /   / /\\ \\ | |\\/| |  __|   \\ \\/  \\/ /| |  | |  _  /|  <   \n");       
        printf("| |    | | \\ \\  / ____ \\| |  | | |____   \\  /\\  / | |__| | | \\ \\| . \\  \n");       
        printf("|_|    |_|  \\_\\/_/   _\\_\\_|  |_|______|   \\/  \\/   \\____/|_|  \\_\\_|\\_\\ \n");       
        printf("\n");
        printf(" _ __ ___   __ _ ___| |_ ___ _ __   ______  __      _____  _ __| | _____ _ __ \n");
        printf("| '_ ` _ \\ / _` / __| __/ _ \\ '__| |______| \\ \\ /\\ / / _ \\| '__| |/ / _ \\ '__|\n");
        printf("| | | | | | (_| \\__ \\ ||  __/ |              \\ V  V / (_) | |  |   <  __/ |   \n");
        printf("|_| |_| |_|\\__,_|___/\\__\\___|_|               \\_/\\_/ \\___/|_|  |_|\\_\\___|_|   \n\n");

        // Creo il thread che prenderà in input le azioni da eseguire e le salverà in actionsToDo
        pthread_create(&slaveThreadMainProcess, NULL, (void*)handlerAction, actionsToDo);

        // Continua fino a quando non è stata espressa la volontà di voler terminare il programma
        while (!exit_)
        {
            // Se sono in attesa di una conferma di avvenuta ricezione dell'azione da parte dei workers e la richiesta è valida
            if (!nextActionLock && tempRequest) {
                // Provo a vedere se la richiesta è stata ricevuta
                MPI_Test(&tempRequest, &nextActionLock, MPI_STATUS_IGNORE);
                tempRequest = MPI_REQUEST_NULL;
            }
            
            // Se ho eseguito una azione che richiede threads per essere eseguita e ho ottenuto lo stato aggiornato 
            if (tempAction != 0 && !actionWithWorkers)
            {
                // Verifico che ci sia almeno un thread per ciascun worker disponibile
                tempFreeThreads = getFreeThreads(status, commSize, numThreads);

                if (tempFreeThreads >= (commSize - 1))
                {
                    action = tempAction;
                    printf("Ho ottenuto dei threads liberi a cui assegnare il task\n\n");
                // altrimenti rieseguo l'azione per ottenere lo status dei workers
                } else {
               
                    action = 1;
                    actionWithWorkers = true;
                }
                result = 0;
            }
            /* Se non ho richieste pendenti e non sto aspettando di ricevere lo status aggiornato dei workers,
             * allora posso estrarre un'altra azione da eseguire */
            else if (nextActionLock && tempAction == 0)
            {
                pthread_mutex_lock(&actionsMutex);
                result = popAction(actionsToDo, &action);
                pthread_mutex_unlock(&actionsMutex);   
            } 

            // Se ho estratto effettivamente una azione da eseguire (valore di ritorno della pop)
            if (!result)
            {
                /* Se l'azione estratta richiede di inserire input da tastiera termino il thread
                 * (lo slave del main thread) che era bloccato in attesa di input da tastiera */
                if (action == 5) pthread_cancel(slaveThreadMainProcess);

                /* L'azione 4 e la 6 richiedono thread per essere eseguiti quindi se non sono in attesa di ottenere
                 * lo status aggiornato dei workers allora lo richiedo per loro */
                if ((action == 4 || action == 6) && tempAction == 0)
                {
                    pthread_cancel(slaveThreadMainProcess);
                    actionWithWorkers = true;
                    tempAction = action;
                    action = 1;
                    pthread_mutex_lock(&stdoutMutex);
                    printf("Sto ottenendo threads liberi per eseguire i task, finchè non li avrò ottenuti i task inseriti verranno accodati\n");
                    pthread_mutex_unlock(&stdoutMutex);
                }

                // Se non ho richieste pendenti e l'azione tenuta in action è valida
                if (nextActionLock && action != 0) {
                    // Trasmetto l'azione da effettuare ai workers
                    MPI_Ibcast(&action, 1, MPI_INT, 0, MPI_COMM_WORLD, &tempRequest);
                    nextActionLock = false;
                } 
                
                switch(action)
                {   
                    case 6:
                    // Se è stata creata almeno una matrice
                    if (matricesLen >= 1) {
                        // Stampo il resoconto delle matrici create
                        printMatrices(matrices, matricesLen);
                        if (matricesLen == 1)
                        {
                            printf("Sommero la matrice 0 per se stessa\n");
                            temp = 0;
                            tempS = 0;
                        }
                        else 
                        {   
                            // Acquisizione di input necessari per eseguire l'azione
                            do
                            {
                                adjust = false;
                                printf("Qual è la matrice a cui sommare l'altra matrice?\n");
                                scanf("%d", &temp);
                                printf("Qual è la matrice da sommare?\n");
                                scanf("%d", &tempS);
                                if (temp < 0 || tempS < 0 || temp >= matricesLen || tempS >= matricesLen || matrices[temp].numCols != matrices[tempS].numCols || matrices[temp].numRows != matrices[tempS].numRows) {
                                    printf("Hai inserito dei numeri di matrice invalidi!\n");
                                    printf("Le matrici interessate devono avere lo stesso numero di colonne e di righe, potresti utilizzare anche la stessa matrice\n");
                                    adjust = true;
                                }
                            } while(adjust);
                            printf("Sommerò le matrici %d e %d\n", temp, tempS);
                        }
                        // Ottengo il numero di threads disponibili
                        tempFreeThreads = getFreeThreads(status,commSize, numThreads);
                        while (threadsUsed < commSize - 1 || threadsUsed > tempFreeThreads)
                        {
                            printf("Quanti thread vuoi usare per svolgere questo compito? Scegli un valore da %d a un massimo %d\n", commSize - 1, tempFreeThreads);
                            getIntFromStdin(&threadsUsed);
                        }
                        // Una volta ottenuti tutti gli input mi segno il tempo di inizio dell'esecuzione
                        startTime = MPI_Wtime();

                        // Calcolo il numero di elementi che dovranno essere assegnati a ciascun thread
                        tempNumElements = tempMatrix->size / threadsUsed;
                        extraElements = tempMatrix->size - (tempNumElements * threadsUsed);

                        // Inizializzo la porzione di memoria a 0
                        memset(argsToWorker, 0, sizeof(msgToWorker) * (commSize - 1));

                        /* Calcolo il numero di threads che saranno utilizzati da ciascun worker, la size da eseguire (il carico di lavoro)
                         * e quindi inzializzo l'array contenente le strutture 'argsToWorker' che dovranno essere trasmesse ai workers attraverso MPI */
                        for(int i = 1; i < numThreads && threadsUsed; i++)
                        {
                            for (int j = 1; j < commSize && threadsUsed; j++)
                            {
                                if (!status[j - 1].threads[i])
                                {
                                    argsToWorker[j - 1].numThreads++;
                                    argsToWorker[j - 1].size += tempNumElements;

                                    if (extraElements) {
                                        argsToWorker[j - 1].size++;
                                        extraElements--;
                                    }
                                    threadsUsed--;
                                }
                            } 
                        }
                        
                        for (int i = 0; i < commSize - 1; i++)
                        {
                            // Propago argsToWorker ai workers
                            MPI_Send(&argsToWorker[i], 1, MPI_MSG_TO_WORKER, i + 1, 0, MPI_COMM_WORLD);
                            counts[i + 1] = argsToWorker[i].size;
                            displacements[i + 1] =  displacements[i] + counts[i];
                        }

                        // Invio le matrici di cui effettuare la somma
                        MPI_Scatterv(matrices[temp].pointerMatrix, counts, displacements, MPI_INT, NULL, 0, MPI_INT, 0, MPI_COMM_WORLD);
                        MPI_Scatterv(matrices[tempS].pointerMatrix, counts, displacements, MPI_INT, NULL, 0, MPI_INT, 0, MPI_COMM_WORLD);

                        // Ricevo le matrice risultante dell'operazione somma
                        MPI_Gatherv(NULL, 0, MPI_INT, matrices[temp].pointerMatrix, counts, displacements, MPI_INT, 0, MPI_COMM_WORLD);

                        endTime = MPI_Wtime();

                        printf("Somma tra matrici conclusa...\nTempo impiegato: %f\n", endTime-startTime);

                    } else printf("Non hai ancora generato almeno due matrici!\n");

                    // Senza questo non posso rieseguire la stessa azione
                    threadsUsed = 0; 
                    // Ora posso riniziare ad acquisire azioni da eseguire
                    tempAction = 0;
                    // Creo il thread cancellato in precedenza per poter riniziare ad acquisire le azioni da voler programmare
                    pthread_create(&slaveThreadMainProcess, NULL, (void*)handlerAction, actionsToDo);
                    break;
                    case 5:
                    if (matricesLen) {

                        printMatrices(matrices, matricesLen);

                        printf("Sono state create %d matrici, seleziona la matrice da stampare:\nInserisci un numero da 0 a %d, dove 0 è la prima matrice creata, %d l'ultima.\n", matricesLen, matricesLen - 1, matricesLen - 1);
                        scanf("%d", &temp);

                        while(temp < 0 || temp > matricesLen-1)
                        {
                            printf("Hai immesso un numero della matrice da stampare invalido, inseriscine un altro\n");
                            scanf("%d", &temp);
                        } 

                        printMatrix(matrices[matricesLen - 1].pointerMatrix, matrices[matricesLen - 1].numRows, matrices[matricesLen - 1].numCols);

                    } else printf("Ancora non hai generato una matrice!\n");

                    pthread_create(&slaveThreadMainProcess, NULL, (void*)handlerAction, actionsToDo);
                    break;
                    case 4:

                    do {
                        printf("\nInserisci 1 se vuoi creare una nuova matrice altrimenti -1 se desideri rimpiazzare una matrice precedentemente creata:\n");
                        getIntFromStdin(&temp);
                    } while(temp != -1 && temp != 1);

                    do {
                        if (adjust) {
                            temp = 1;
                            adjust = false;
                        } 
                        if (temp == 1) 
                        {
                            printf("Aggiungo una nuova matrice\n");
                            matricesLen++;
                            if (matricesLen == 1) matrices = malloc(sizeof(matrix));
                            else matrices = realloc(matrices, sizeof(matrix) * matricesLen);
                            tempMatrix = &matrices[matricesLen - 1];
                        }
                        else if (temp == -1 && matricesLen > 0) {
                            if (matricesLen == 1)
                            {
                                printf("Rimpiazzo la matrice precedentemente creata.\n");
                                temp = 0;
                                tempFree = true;
                            }
                            else {
                                printf("Sono state create %d matrici, seleziona la matrice da rimpiazzare:\nInserisci un numero da 0 a %d, dove 0 è la prima matrice creata, %d l'ultima.\n", matricesLen, matricesLen - 1, matricesLen - 1);
                                scanf("%d", &temp);
                                tempFree = true;
                            }
                            while(temp < 0 || temp >= matricesLen) {
                                printf("Seleziona correttamente il numero della matrice da rimpiazzare!\n");
                                getIntFromStdin(&temp);
                            }
                            if (tempFree) {
                                tempMatrix = &matrices[temp];
                                free(tempMatrix->pointerMatrix);
                            }
                        }
                        else if(temp == -1 && matricesLen == 0) {
                            printf("Non ci sono matrici precedentemente memorizzate!\n");
                            adjust = true;
                        }
                    } while ((adjust));

                    do
                    {
                        adjust = false;
                        printf("Da quante righe deve essere composta la matrice?\n");
                        getIntFromStdin(&(tempMatrix->numRows));
                        printf("Da quante colonne deve essere composta la matrice?\n");
                        getIntFromStdin(&(tempMatrix->numCols));
                        if (tempMatrix->numRows < 0 || tempMatrix->numCols < 0 || tempMatrix->numRows * tempMatrix->numCols < commSize - 1) {
                            printf("Hai inserito un valore invalido! Reinserisci i valori.\n");
                            adjust = true;
                        }
                    } while(adjust);
                    
                    tempMatrix->size = tempMatrix->numRows * tempMatrix->numCols;
        
                    tempMatrix->pointerMatrix = malloc((size_t)(sizeof(int) * tempMatrix->size));

                    if (!tempMatrix->pointerMatrix)
                    {
                        printf("La malloc non è andata a buon fine... forse sarà perchè hai inserito un numero di colonne o di righe troppo grande.\n");
                        MPI_Abort(MPI_COMM_WORLD, 1);
                    }
               
                    tempFreeThreads = getFreeThreads(status,commSize, numThreads);
                    while (threadsUsed < commSize - 1 || threadsUsed > tempFreeThreads)
                    {
                        printf("Quanti thread vuoi usare per svolgere questo compito? Scegli un valore da %d a un massimo %d\n", commSize - 1, tempFreeThreads);
                        getIntFromStdin(&threadsUsed);
                    }

                    printMatrices(matrices, matricesLen);

                    tempNumElements = tempMatrix->size / threadsUsed;
                    extraElements = tempMatrix->size - (tempNumElements * threadsUsed);

                    memset(argsToWorker, 0, sizeof(msgToWorker) * (commSize - 1));

                    for(int i = 1; i < numThreads && threadsUsed; i++)
                    {
                        for (int j = 1; j < commSize && threadsUsed; j++)
                        {
                            if (!status[j - 1].threads[i])
                            {
                                argsToWorker[j - 1].numThreads++;
                                argsToWorker[j - 1].size += tempNumElements;

                                if (extraElements) {
                                    argsToWorker[j - 1].size++;
                                    extraElements--;
                                }
                                threadsUsed--;
                            }
                        } 
                    }
                    
                    tempListWorker = malloc(sizeof(listWorkers) * (commSize - 1));

                    for (int i = 0; i < commSize - 1; i++)
                    {
                        MPI_Send(&argsToWorker[i], 1, MPI_MSG_TO_WORKER, i + 1, 0, MPI_COMM_WORLD);
                        tempListWorker[i].rank = i + 1;
                        tempListWorker[i].size = argsToWorker[i].size;
                        tempListWorker[i].status = 0;
                        tempListWorker[i].request = MPI_REQUEST_NULL;
                    }

                    // Aggiungo una nuova azione da eseguire
                    addPendingActions(&pa, &pa_size, action, commSize - 1, tempListWorker);
   
                    // Senza questo non posso rieseguire la stessa azione
                    threadsUsed = 0; 
                    // Ora posso riniziare ad acquisire azioni da eseguire
                    tempAction = 0;
                    pthread_create(&slaveThreadMainProcess, NULL, (void*)handlerAction, actionsToDo);

                    break;
                    case 2:
                    pthread_mutex_lock(&actionsMutex);
                    printPendingActions(pa, pa_size);
                    pthread_mutex_unlock(&actionsMutex);
                    break;
                    case 1:                    
                    tempListWorker = malloc(sizeof(listWorkers) * (commSize - 1));

                    for(int i = 0; i < commSize - 1; i++) {
                        tempListWorker[i].rank = i + 1;
                        tempListWorker[i].size = numThreads;
                        tempListWorker[i].status = 0;
                        tempListWorker[i].request = MPI_REQUEST_NULL;
                    }

                    addPendingActions(&pa, &pa_size, action, commSize - 1, tempListWorker);
                    if (actionWithWorkers) pa[pa_size - 1].publish = true;
                    else pa[pa_size - 1].startTime = MPI_Wtime();
                    break;
                    case -1:
                    exit_ = true;
                    break;
                    case -2:
                    MPI_Abort(MPI_COMM_WORLD, 0);
                    break;
                }
                action = 0;
            }

            /* Itero sulle azioni e se non sono complete e se su i worker partecipanti non risultano richieste pendenti provvedo a 
             * fare una richiesta nei loro confronti e a salvarmi lo stato di tali richieste */
            for(int i = 0; i < pa_size; i++)
            {
                if (!(pa[i].completed))
                {
                    switch(pa[i].action)
                    {
                        case 4:
                        temp = 0;
                        for(int j = 0; j < pa[i].numWorkers; j++)
                        {
                            if(!pa[i].workers[j].status && pa[i].workers[j].request == MPI_REQUEST_NULL)
                            {
                                MPI_Irecv((matrices[matricesLen-1].pointerMatrix + temp), pa[i].workers[j].size, MPI_INT, pa[i].workers[j].rank, 4, MPI_COMM_WORLD, &(pa[i].workers[j].request));
                                temp += pa[i].workers[j].size;    
                            }
                                
                        }
                        break;
                        case 1:
                        for(int j = 0; j < pa[i].numWorkers; j++)
                        {
                            if(!pa[i].workers[j].status && pa[i].workers[j].request == MPI_REQUEST_NULL) 
                                MPI_Irecv(status[j].threads, numThreads, MPI_INT, pa[i].workers[j].rank, 1, MPI_COMM_WORLD, &(pa[i].workers[j].request));
                        }  
                        break;
                    }
                }
            }

            /* Itero sulle azioni in pendingAction e se l'azione non è completa controllo che tutte le request associate ad essa e 
             * quindi i worker lo siano, se vero allora l'action è completata, altrimenti no */
            for(int i = 0; i < pa_size; i++)
            {
                if (!(pa[i].completed))
                {
                    completed = true;
                    if (pa[i].action == 4 || pa[i].action == 1)
                    {
                        for(int j = 0; j < pa[i].numWorkers; j++)
                        {
                            if (!(pa[i].workers[j].status))
                            {
                                MPI_Test(&(pa[i].workers[j].request), &flag, &statusTest);
                                if(flag) 
                                {
                                    status[j].rank = statusTest.MPI_SOURCE;
                                    pa[i].workers[j].status = true;
                                } else { completed = false; }
                            } 
                        }
                    }
                    if (completed) {
                        pa[i].completed = true;

                        if (actionWithWorkers && pa[i].action == 1) {
                            actionWithWorkers = false;
                        }
                
                        free(pa[i].workers);
                        pa[i].workers = NULL;
                    }
                }
            }

            // Itero sulle azioni in prendingAction e se non risultano essere ancora stampate a video ma risultano essere completate le stampo
            for(int i = 0; i < pa_size; i++)
            {
                if (pa[i].completed && !(pa[i].publish))
                {
                    switch(pa[i].action)
                    {
                        case 4:
                        pthread_mutex_lock(&stdoutMutex);
                        printf("Creazione della matrice completata...\nTempo impiegato: %f\n", MPI_Wtime() - pa[i].startTime);
                        pthread_mutex_unlock(&stdoutMutex);
                        break;
                        case 1:
                        pthread_mutex_lock(&stdoutMutex);
                        printf("=======STATO DEI THREADS=======\n");
                        for(int i = 0; i < commSize - 1; i++)
                        {
                            printf("Stato attuale processo %d\n", status[i].rank);
                            for(int j = 0; j < numThreads; j++)
                                printf(" - Thread %d stato %d\n", j, status[i].threads[j]);
                        }
                        pthread_mutex_unlock(&stdoutMutex);
                        //printf("\nHo ottenuto lo stato dei workers...\nTempo impiegato: %f\n", MPI_Wtime() - pa[i].startTime);
                        break;
                    }
                    pa[i].publish = true;
                }
            }                
        }

        if (matricesLen > 0) {
            for(int i = 0; i < matricesLen; i++) 
                free(matrices[i].pointerMatrix);
            free(matrices);
        }
            
        for(int i = 0; i < commSize - 1; i++) free(status[i].threads);
        free(displacements);
        free(counts);
        free(status);  
        free(pa);
        free(actionsToDo);
        pthread_detach(slaveThreadMainProcess);
        pthread_mutex_destroy(&actionsMutex);
    }
    // Sono un worker
    else
    { 
        MPI_Request request;
        MPI_Status status;
        
        // Ottengo il numero di thread da dover utilizzare
        MPI_Bcast(&numThreads, 1, MPI_INT, 0, MPI_COMM_WORLD);

        // Istanzio un array globale in cui i thread aggiornare il loro stato
        threadStatusSlaveProcess = malloc(sizeof(int) * numThreads);
        for(int i = 1; i < numThreads; i++) threadStatusSlaveProcess[i] = 0;
        threadStatusSlaveProcess[0] = 1;

        /* Struttura utilizzata per tenere il tid associato al thread e per sapere se tale thread è stato mai utilizzato o 
         * meno cosi che finita l'esecuzione posso liberarne le risorse */
        typedef struct 
        {
            pthread_t thread;
            bool neverUsed;
        } threads;

        threads * slaveThreads = malloc(sizeof(threads) * (numThreads - 1));
        for(int i = 0; i < numThreads - 1; i++) slaveThreads[i].neverUsed = true;

        // Pointer alla struttura in cui memorizzerò i dati ottenuti dal master utili a eseguire una azione
        msgToThread * msg = malloc(sizeof(msgToThread) * (numThreads - 1));
        for(int i = 0; i < numThreads - 1; i++)
        {
            msg[i].pid = rank;
            msg[i].tid = i + 1;
        }

        int nextExecution = 0;

        argsToWorker = calloc(sizeof(msgToWorker), 1);
        msgToWorker copyArgsToWorker;

        int threadsUsed[numThreads - 1];
        tempMatrix = malloc(sizeof(int));
        tempMatrixS = malloc(sizeof(int));

        work * works = malloc(sizeof(work));
        int works_size = 1;
        works[0].completed = true;
        works[0].threads = malloc(sizeof(int));
        int * tempThreads;

        while(!exit_)
        {
            usleep(5000);
            for(int i = 0; i < works_size; i++)
            {
                if (!(works[i].completed) && !(works[i].send))
                {
                    //printf("CONTROLLO --- numThreads: %d\n", works[i].numThreads);
                    completed = true;
                    for(int j = 0; j < works[i].numThreads; j++) {
                        //printf("STATUS THREAD: %d --- %d\n", works[i].threads[j], threadStatusSlaveProcess[works[i].threads[j]]);
                        if (threadStatusSlaveProcess[works[i].threads[j]]) {
                            completed = false;
                            //printf("NON HANNO FINITO TUTTI\n");
                            break;
                        }
                    }  
                } else if (!(works[i].completed) && works[i].send) 
                {
                    MPI_Test(&(works[i].request), &flag, MPI_STATUS_IGNORE);
                    if (flag) {
                        works[i].completed = true;
                        //printf("INVIO AVVENUTO CORRETTAMENTE\n");
                    }
                }

                if (completed)
                {
                    switch (works[i].action)
                    {
                        case 4:
                        //printf("INVIATO\n");
                        MPI_Isend(works[i].matrix, works[i].size, MPI_INT, 0, works[i].action, MPI_COMM_WORLD, &(works[i].request));
                        works[i].send = true;
                        break;
                    }
                    completed = false;
                }
            }

            if (!nextActionLock) {
                MPI_Test(&tempRequest, &nextActionLock, &status);
                nextExecution = nextActionLock;
            }

            if (nextActionLock) 
            {
                //printf("Ora che ho mandato in esec l'azione ne posso prendere un'altra\n");
                MPI_Ibcast(&action, 1, MPI_INT, 0, MPI_COMM_WORLD, &tempRequest);
                nextActionLock = false;
            }

            if (nextExecution)
            {

                switch (action)
                {
                    case 6:
                    MPI_Recv(argsToWorker, 1, MPI_MSG_TO_WORKER, 0, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    copyArgsToWorker = *argsToWorker;

                    tempMatrix = realloc(tempMatrix, sizeof(int) * argsToWorker->size);
                    tempMatrixS = realloc(tempMatrixS, sizeof(int) * argsToWorker->size);

                    memset(threadsUsed, 0, sizeof(int) * (numThreads - 1));

                    tempNumElements = argsToWorker->size / argsToWorker->numThreads;
                    extraElements = argsToWorker->size - (tempNumElements * argsToWorker->numThreads);

                    temp = 0;

                    MPI_Scatterv(NULL, NULL, NULL, MPI_INT, tempMatrix, argsToWorker->size, MPI_INT, 0, MPI_COMM_WORLD);
                    MPI_Scatterv(NULL, NULL, NULL, MPI_INT, tempMatrixS, argsToWorker->size, MPI_INT, 0, MPI_COMM_WORLD);

                    for(int i = 1; i < numThreads && temp != argsToWorker->numThreads; i++)
                    {
                        if(!threadStatusSlaveProcess[i])
                        {
                            if (slaveThreads[i - 1].neverUsed) slaveThreads[i - 1].neverUsed = false;
                            threadsUsed[temp] = i;

                            if (temp > 0) {
                                msg[temp].matrix = msg[temp - 1].matrix + msg[temp - 1].size;
                                msg[temp].displacements = msg[temp - 1].displacements + msg[temp - 1].size;
                            }
                            else {
                                msg[temp].matrix = tempMatrix;
                                msg[temp].displacements = 0;
                            }

                            msg[temp].size = tempNumElements;

                            if (extraElements > 0)
                            {
                                msg[temp].size++;
                                extraElements--;
                            }

                            temp++;
                        }
                    }
                    
                    for (int i = 0; i < temp; i++)
                        pthread_create(&(slaveThreads[threadsUsed[i] - 1].thread), NULL, (void*)sumMatrix, &msg[i]);
                    
                    for (int i = 0; i < temp; i++)
                        pthread_join(slaveThreads[threadsUsed[i] - 1].thread, NULL);
                    
                    MPI_Gatherv(tempMatrix, copyArgsToWorker.size, MPI_INT, NULL, NULL, NULL, MPI_INT, 0, MPI_COMM_WORLD);
                    
                    break;
                    case 4:
                    MPI_Recv(argsToWorker, 1, MPI_MSG_TO_WORKER, 0, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    copyArgsToWorker = *argsToWorker;
                    tempMatrix = malloc(sizeof(int) * argsToWorker->size);
                    memset(threadsUsed, 0, sizeof(int) * (numThreads - 1));

                    tempNumElements = argsToWorker->size / argsToWorker->numThreads;
                    extraElements = argsToWorker->size - (tempNumElements * argsToWorker->numThreads);

                    temp = 0;

                    for(int i = 1; i < numThreads && temp != argsToWorker->numThreads; i++)
                    {
                        if(!threadStatusSlaveProcess[i])
                        {
                            if (slaveThreads[i - 1].neverUsed) slaveThreads[i - 1].neverUsed = false;
                            threadsUsed[temp] = i;

                            if (temp > 0) msg[temp].matrix = msg[temp - 1].matrix + msg[temp - 1].size;
                            else msg[temp].matrix = tempMatrix;

                            msg[temp].size = tempNumElements;

                            if (extraElements > 0)
                            {
                                msg[temp].size++;
                                extraElements--;
                            }
                            temp++;
                        }
                    }

                    tempThreads = malloc(sizeof(int) * temp);

                    for (int i = 0; i < temp; i++) {
                        pthread_create(&(slaveThreads[threadsUsed[i] - 1].thread), NULL, (void*)generateMatrix, &msg[i]);
                        tempThreads[i] = threadsUsed[i];
                    }
                        
                    addWork(&works, &works_size, 4, copyArgsToWorker.size, tempMatrix, temp, tempThreads);

                    break;
                    case 3:
                    for(int i = 0; i < numThreads - 1; i++)
                    {
                        if(!threadStatusSlaveProcess[i + 1])
                        {
                            if (slaveThreads[i].neverUsed) slaveThreads[i].neverUsed = false;
                            pthread_create(&(slaveThreads[i].thread), NULL, (void*)helloWorld, &msg[i]);
                        }
                    }
                    break;
                    case 1:
                    MPI_Isend(threadStatusSlaveProcess, numThreads, MPI_INT, 0, 1, MPI_COMM_WORLD, &request);
                    //printf("Sono il rank %d e ho inviato\n", rank);
                    break;
                    case -1:
                    exit_ = true;
                    break;
                }
                nextExecution = 0;
            }
        }

        for(int i = 0; i < numThreads - 1 && !(slaveThreads[i].neverUsed); i++) pthread_detach(slaveThreads[i].thread);
        free(slaveThreads);
        free(threadStatusSlaveProcess);
        free(msg);
    }
    free(argsToWorker);
    free(tempMatrix);
    free(tempMatrixS);
    MPI_Type_free(&MPI_MSG_TO_WORKER);
    MPI_Finalize();
    return EXIT_SUCCESS;
}
#include <sys/stat.h>
#include <getopt.h>
#include <sys/types.h>
#include <unistd.h>
#include <math.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <semaphore.h>
#include <sys/socket.h> 
#include <netinet/in.h> 

#define MAX_CLIENT_SIZE 1000
#define CELL_SIZE 4096

typedef unsigned long long timestamp_t;

int inputfile=0;
int outputfile=0;
int pidfile=0;
int row_size=0;
int column_size=0;
int poolsize=0;
char *** db;
int new_fd=0;
pthread_t * threads;
pthread_attr_t * thread_data;
struct queue* connection_queue;
pthread_mutex_t mutex;
pthread_cond_t cond_variable;

struct queue { 
    int start, end, size; 
    int capacity; 
    int * array; 
};

struct queue* create_queue(int capacity);
int is_full(struct queue* the_queue);
int is_empty(struct queue* the_queue);
void push_back(struct queue* the_queue, int element);
int pop_front(struct queue* the_queue);
int front(struct queue* the_queue);
int back(struct queue* the_queue);
static timestamp_t get_timestamp();
void get_local_time(char * string);
void * threadFunc(void * parameters);
void SIGINT_handler(int signa);

int main(int argc, char ** argv){

    int i=0,j=0,k=0;
    int parser = 0;
    int parser_number=0;
    int port=0;
    char printformat[10000];
    char logfile[10000];
    char datasetfile[10000];
    char date_string[1000];
    int sw_fd; 
    struct sockaddr_in address; 
    int opt = 1; 
    int addrlen = sizeof(address);
    int inputcheck[4]={0,0,0,0};

    pidfile = open("/tmp/instance.pid", O_CREAT | O_EXCL, 0666);
    if(pidfile == -1){
        perror("Open error:");
        exit(EXIT_FAILURE);
    }

    while((parser = getopt(argc, argv, "p:o:l:d:")) != -1)   
    {  
        switch(parser)  
        {  
            case 'p':
            {
                if(inputcheck[0]>=1){
                    perror("Same input twice.\n");
                    exit(EXIT_FAILURE);
                }
                sscanf(optarg,"%d",&port);
                inputcheck[0]++;  
                break;
            }  
            case 'o':
            {
                if(inputcheck[1]>=1){
                    perror("Same input twice.\n");
                    exit(EXIT_FAILURE);
                }
                outputfile = open(optarg,O_RDWR | O_CREAT | O_SYNC,0666);
                if(outputfile == -1){
                    perror("Open error");
                    exit(EXIT_FAILURE);
                }
                sprintf(logfile,"%s",optarg);
                inputcheck[1]++;
                break; 
            }
            case 'l':
            {
                if(inputcheck[2]>=1){
                    perror("Same input twice.\n");
                    exit(EXIT_FAILURE);
                }
                inputcheck[2]++;
                sscanf(optarg,"%d",&poolsize);  
                break;
            }
            case 'd':
            {
                if(inputcheck[3]>=1){
                    perror("Same input twice.\n");
                    exit(EXIT_FAILURE);
                }
                inputfile = open(optarg,O_RDONLY,0666);
                if(inputfile == -1){
                    perror("Open error");
                    exit(EXIT_FAILURE);
                }
                sprintf(datasetfile,"%s",optarg);
                inputcheck[3]++;  
                break;
            }
            default:
            {
                perror("The command line arguments are missing/invalid.\n");
                exit(EXIT_FAILURE);
            }
            break;
        }
        parser_number++;  
    }
    if(parser_number!=4){
        perror("Too many/less arguments.\n");
        exit(EXIT_FAILURE);
    }

    if(poolsize<=1){
        fprintf(stderr,"Thread pool must be larger than 1.\n");
        exit(EXIT_FAILURE);
    }

    signal(SIGINT,SIGINT_handler);
    daemon(0,0);

    get_local_time(date_string);
    sprintf(printformat,"%s Executing with parameters\n%s -p %d\n%s -o %s\n%s -l %d\n%s -d %s\n",date_string,date_string,port,date_string,logfile,date_string,poolsize,date_string,datasetfile);
    write(outputfile,printformat,strlen(printformat));

    get_local_time(date_string);
    sprintf(printformat,"%s Loading dataset...\n",date_string);
    write(outputfile,printformat,strlen(printformat));
    timestamp_t dataset_start = get_timestamp();
    char c;
    j=1;
    int inside_string=0;
    do{
        if(read(inputfile,&c,1)==-1){
            perror("Read Error");
            exit(EXIT_FAILURE);
        }
        if(c=='"' && inside_string==0){
            inside_string=1;
        }
        else if(c=='"' && inside_string==1){
            inside_string=0;
        }
        else if(c==',' && inside_string==0){
            j++;
        }
    }
    while(c!='\n');
    int end=lseek(inputfile,0,SEEK_END);
    int curr=lseek(inputfile,0,SEEK_SET);
    i=0;
    do{
        if(read(inputfile,&c,1)==-1){
            perror("Read Error");
            exit(EXIT_FAILURE);
        }
        if(c=='\n'){
            i++;
        }
        curr=lseek(inputfile,0,SEEK_CUR);
    }
    while(curr<end);
    row_size=i;
    column_size=j;

    db = (char ***)malloc(sizeof(char **)*row_size);
    for (i=0; i<row_size; i++){
        db[i] = (char **)malloc(sizeof(char *)*column_size);
        for(j=0; j<column_size; j++){
            db[i][j] = (char *)malloc(sizeof(char)*CELL_SIZE);
        }
    }

    curr=lseek(inputfile,0,SEEK_SET);
    char str[CELL_SIZE];
    i=0;
    do{
        j=0;
        k=0;
        do{
            if(read(inputfile,&c,1)==-1){
                perror("Read Error");
                exit(EXIT_FAILURE);
            }
            if(c=='"' && inside_string==0){
                inside_string=1;
            }
            else if(c=='"' && inside_string==1){
                inside_string=0;
            }
            else if((c==',' || c=='\n') && inside_string==0){
                if(str[k-1]=='\r'){
                    str[k-1]='\0';
                }
                else{
                    str[k]='\0';
                }
                strcpy(db[i][j],str);
                j++;
                k=0;
            }
            else{
                str[k]=c;
                k++;
            }
        }
        while(c!='\n');
        i++;
        curr=lseek(inputfile,0,SEEK_CUR);
    }
    while(curr<end);

    timestamp_t dataset_end = get_timestamp();
    double read_time = (dataset_end - dataset_start) / 1000000.0L;

    get_local_time(date_string);
    sprintf(printformat,"%s Dataset loaded in %lf seconds with %d records.\n",date_string,read_time,row_size);
    write(outputfile,printformat,strlen(printformat));

    sw_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sw_fd == -1){ 
        perror("Socket is failed"); 
        return -1; 
    }

    if (setsockopt(sw_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt)) == -1){ 
        perror("Setsockopt is failed"); 
        return -1; 
    } 
    address.sin_family = AF_INET;  
    address.sin_port = htons(port);
    address.sin_addr.s_addr = INADDR_ANY;

    if (bind(sw_fd,(struct sockaddr *)&address,sizeof(address)) == -1){ 
        get_local_time(date_string);
        sprintf(printformat,"%s Bind failed\n",date_string);
        write(outputfile,printformat,strlen(printformat));
        return -1;  
    }

    if (listen(sw_fd, 3) < 0){ 
        perror("Listen is failed."); 
        return -1;  
    }

    connection_queue = create_queue(MAX_CLIENT_SIZE);
    threads=malloc(sizeof(pthread_t)*poolsize);
    thread_data=malloc(sizeof(pthread_attr_t)*poolsize);
    pthread_mutex_init(&mutex,NULL);
    pthread_cond_init(&cond_variable,NULL);

    for(i=0;i<poolsize;i++){
        pthread_attr_init(&thread_data[i]);
        pthread_attr_setdetachstate(&thread_data[i], PTHREAD_CREATE_DETACHED);
        if(pthread_create(&threads[i],&thread_data[i],threadFunc,(void *)NULL)<0){
            perror("pthread_create error");
            return -1;
        }
    }
    get_local_time(date_string);
    sprintf(printformat,"%s A pool of %d threads has been created.\n",date_string,poolsize);
    write(outputfile,printformat,strlen(printformat));

    while(1){
        new_fd = accept(sw_fd,(struct sockaddr *)&address,(socklen_t*)&addrlen);
        if (new_fd==-1){ 
            perror("Accept is failed"); 
            return -1;  
        }
        pthread_mutex_lock(&mutex);
        push_back(connection_queue,new_fd);
        pthread_cond_signal(&cond_variable);
        pthread_mutex_unlock(&mutex);
    }

}

struct queue* create_queue(int capacity){ 
    struct queue * the_queue = malloc(sizeof(struct queue)); 
    the_queue->capacity = capacity;
    the_queue->start = the_queue->size = 0; 
  
    the_queue->end = capacity - 1; 
    the_queue->array = (int*)malloc(the_queue->capacity * sizeof(int)); 
    return the_queue; 
} 

int is_full(struct queue* the_queue){ 
    return (the_queue->size == the_queue->capacity); 
} 
  
int is_empty(struct queue* the_queue){ 
    return (the_queue->size == 0); 
} 

void push_back(struct queue* the_queue, int element){ 
    if (is_full(the_queue)){
        return;
    } 
    the_queue->end = (the_queue->end + 1) % the_queue->capacity; 
    the_queue->array[the_queue->end] = element; 
    the_queue->size = the_queue->size + 1;
}

int pop_front(struct queue* the_queue){ 
    if (is_empty(the_queue)){
        return -1;
    }
    int element = the_queue->array[the_queue->start]; 
    the_queue->start = (the_queue->start + 1) % the_queue->capacity; 
    the_queue->size = the_queue->size - 1; 
    return element; 
}

int front(struct queue* the_queue){ 
    if (is_empty(the_queue)){
        return -1;
    } 
    return the_queue->array[the_queue->start]; 
} 

int back(struct queue* the_queue){ 
    if (is_empty(the_queue)){
        return -1;
    } 
    return the_queue->array[the_queue->end]; 
} 

int is_contains(struct queue* the_queue,int element){ 
    int i=0;
    for(i=0; i<the_queue->size; i++){
        if(the_queue->array[i]==element){
            return 1;
        }
    }
    return 0; 
}

static timestamp_t get_timestamp(){
    struct timeval now;
    gettimeofday (&now, NULL);
    return  now.tv_usec + (timestamp_t)now.tv_sec * 1000000;
}

void get_local_time(char * string){
    time_t time_type = time(NULL);
    struct tm *time_tm = localtime(&time_type);
    strftime(string,30,"%F %T",time_tm);
}

void SIGINT_handler(int signa){
    free(connection_queue->array);
    free(connection_queue);
    int i=0,j=0;
    for (i=0; i<poolsize; i++){
        pthread_join(threads[i],NULL);
    }
    for (i=0; i<row_size; i++){
        for(j=0; j<column_size; j++){
            free(db[i][j]);
        }
    }
    for (i=0; i<row_size; i++){
        free(db[i]);
    }
    free(db);
    write(outputfile,"SIGINT catched. Terminating...\n",sizeof("SIGINT catched. Terminating...\n")+1);
    for (i=0; i<poolsize; i++){
        pthread_attr_destroy(&thread_data[i]);
    }
    free(threads);
    free(thread_data);
    close(pidfile);
    remove("/tmp/instance.pid");
    exit(EXIT_SUCCESS);
}

void * threadFunc(void * parameters){
    static int thread_number=0;
    int thread_current=thread_number;
    thread_number++;
    pthread_detach(pthread_self());

    char printformat[10000];
    char client_string[10000];
    char date_string[1000];
    int i=0,j=0,k=0;
    int connect_fd=0;
    while(1){
        get_local_time(date_string);
        sprintf(printformat,"%s Thread #%d: waiting for connection\n",date_string,thread_current);
        write(outputfile,printformat,strlen(printformat));
        pthread_mutex_lock(&mutex);
        connect_fd=pop_front(connection_queue);
        if(connect_fd==-1){
            pthread_cond_wait(&cond_variable,&mutex);
            connect_fd=pop_front(connection_queue);
        }
        pthread_mutex_unlock(&mutex);
        get_local_time(date_string);
        sprintf(printformat,"%s A connection has been delegated to thread id #%d\n",date_string,thread_current);
        write(outputfile,printformat,strlen(printformat));

        int size_of_queries=0;
        if(recv(connect_fd, &size_of_queries, sizeof(int), 0) == -1){
            perror("Recv error.");
            pthread_exit(NULL);
        }

        int counter=0;
        for(counter=0;counter<size_of_queries;counter++){
            int size_of_s=0;
            if(recv(connect_fd, &size_of_s, sizeof(int), 0) == -1){
                perror("Recv error.");
                pthread_exit(NULL);
            }

            if(recv(connect_fd, client_string, size_of_s, 0) == -1){
                perror("Recv error.");
                pthread_exit(NULL);
            }
            client_string[size_of_s]='\0';
            i=0;
            j=0;
            char clean_string[10000];
            do{
                if(client_string[i]==' ' && client_string[i+1]==' '){
                    clean_string[j]=client_string[i];
                    i++;
                }
                else if(client_string[i]==',' && client_string[i+1]==' '){
                    clean_string[j]=client_string[i];
                    i++;
                }
                else if(client_string[i]==' ' && client_string[i+1]==','){
                    clean_string[j]=client_string[i+1];
                    i++;
                }
                else{
                    clean_string[j]=client_string[i];
                }
                i++;
                j++;
            }
            while(client_string[i]!='\0');
            clean_string[j]='\0';

            char type[1000];
            char temp[1000];
            char temp2[column_size][1000];
            sscanf(client_string,"%s",type);

            if(strcmp(type,"SELECT")==0){
                sscanf(clean_string,"%s %s",type,temp);
                if(strcmp(temp,"DISTINCT")==0){
                    sscanf(clean_string,"SELECT DISTINCT %s FROM TABLE",temp);
                    i=0;
                    j=0;
                    k=0;
                    char already_sent[row_size][10000];
                    int as=0;
                    do{
                        if(temp[i]==',' || temp[i]=='\0'){
                            temp2[k][j]='\0';
                            k++;
                            j=-1;
                        }
                        else{
                            temp2[k][j]=temp[i];
                        }
                        i++;
                        j++;
                    }
                    while(i<=strlen(temp));
                    int selected[k];
                    for(i=0;i<row_size;i++){
                        char send_string[10000];
                        send_string[0]='\0';
                        int flag=0;
                        if(strcmp(temp2[0],"*")==0){
                            int m=0;
                            for(m=0;m<column_size;m++){
                                strcat(send_string,db[i][m]);
                                strcat(send_string,"\t");
                            }
                            int c1=0;
                            for(c1=0;c1<as;c1++){
                                if(strcmp(already_sent[c1],send_string)==0){
                                    flag=1;
                                    break;
                                }
                            }
                            if(flag==0){
                                strcpy(already_sent[as],send_string);
                                as++;
                                int size_of_str=strlen(send_string);
                                if(send(connect_fd,&size_of_str,sizeof(int),0) == -1){
                                    perror("Send error.");
                                    pthread_exit(NULL);
                                }
                                if(send(connect_fd,send_string,strlen(send_string),0) == -1){
                                    perror("Send error.");
                                    pthread_exit(NULL);
                                }
                            }
                            /*SEND ALL TABLE*/
                        }
                        else{
                            int n=0;j=0;
                            while(n<column_size && j<k){
                                if(strcmp(temp2[j],db[0][n])==0){
                                    selected[j]=n;
                                    j++;
                                }
                                n++;
                            }
                            int m=0;
                            for(m=0;m<k;m++){
                                strcat(send_string,db[i][selected[m]]);
                                strcat(send_string,"\t");
                            }
                            int c1=0;
                            for(c1=0;c1<as;c1++){
                                if(strcmp(already_sent[c1],send_string)==0){
                                    flag=1;
                                    break;
                                }
                            }
                            if(flag==0){
                                strcpy(already_sent[as],send_string);
                                as++;
                                int size_of_str=strlen(send_string);
                                if(send(connect_fd,&size_of_str,sizeof(int),0) == -1){
                                    perror("Send error.");
                                    pthread_exit(NULL);
                                }
                                if(send(connect_fd,send_string,strlen(send_string),0) == -1){
                                    perror("Send error.");
                                    pthread_exit(NULL);
                                }
                            }
                            /*SEND SELECTED COLUMN*/
                        }
                    }
                }
                else{
                    sscanf(clean_string,"SELECT %s FROM TABLE",temp);
                    i=0;
                    j=0;
                    k=0;
                    do{
                        if(temp[i]==',' || temp[i]=='\0'){
                            temp2[k][j]='\0';
                            k++;
                            j=-1;
                        }
                        else{
                            temp2[k][j]=temp[i];
                        }
                        i++;
                        j++;
                    }
                    while(i<=strlen(temp));
                    int selected[k];
                    for(i=0;i<row_size;i++){
                        char send_string[10000];
                        send_string[0]='\0';
                        if(strcmp(temp2[0],"*")==0){
                            int m=0;
                            for(m=0;m<column_size;m++){
                                strcat(send_string,db[i][m]);
                                strcat(send_string,"\t");
                            }
                            int size_of_str=strlen(send_string);
                                if(send(connect_fd,&size_of_str,sizeof(int),0) == -1){
                                    perror("Send error.");
                                    pthread_exit(NULL);
                                }
                            if(send(connect_fd,send_string,strlen(send_string),0) == -1){
                                perror("Send error.");
                                pthread_exit(NULL);
                            }
                            /*SEND ALL TABLE*/
                        }
                        else{
                            int n=0;j=0;
                            while(n<column_size && j<k){
                                if(strcmp(temp2[j],db[0][n])==0){
                                    selected[j]=n;
                                }
                                n++;
                                j++;
                            }
                            int m=0;
                            for(m=0;m<k;m++){
                                strcat(send_string,db[i][selected[m]]);
                                strcat(send_string,"\t");
                            }
                            int size_of_str=strlen(send_string);
                                if(send(connect_fd,&size_of_str,sizeof(int),0) == -1){
                                    perror("Send error.");
                                    pthread_exit(NULL);
                                }
                            if(send(connect_fd,send_string,strlen(send_string),0) == -1){
                                perror("Send error.");
                                pthread_exit(NULL);
                            }
                            /*SEND SELECTED COLUMN*/
                        }
                    }
                }
            }
            else if(strcmp(type,"UPDATE")==0){
                char send_string[10000];
                sscanf(clean_string,"UPDATE TABLE SET %s WHERE %s",temp,temp2[0]);
                sprintf(send_string,"UPDATE is not implemented.");
                int size_of_str=strlen(send_string);
                if(send(connect_fd,&size_of_str,sizeof(int),0) == -1){
                    perror("Send error.");
                    pthread_exit(NULL);
                }
                if(send(connect_fd,send_string,size_of_str,0) == -1){
                    perror("Send error.");
                    pthread_exit(NULL);
                }
            }
            else{
                sprintf(printformat,"Unknown command.\n");
                int size_of_str=strlen(printformat);
                if(send(connect_fd,&size_of_str,sizeof(int),0) == -1){
                    perror("Send error.");
                    pthread_exit(NULL);
                }
                if(send(connect_fd,printformat,size_of_str,0) == -1){
                    perror("Send error.");
                    pthread_exit(NULL);
                }
            }

            int size_of_str=strlen("Last message");
            if(send(connect_fd,&size_of_str,sizeof(int),0) == -1){
                perror("Send error.");
                pthread_exit(NULL);
            }
            if(send(connect_fd,"Last message",strlen("Last message"),0) == -1){
                perror("Send error.");
                pthread_exit(NULL);
            }
            usleep(500000);
        }
    }
    pthread_exit(NULL);
}
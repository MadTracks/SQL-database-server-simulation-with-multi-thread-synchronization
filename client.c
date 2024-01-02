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
#include <time.h>
#include <semaphore.h>
#include <sys/socket.h> 
#include <arpa/inet.h> 
#include <string.h>

typedef unsigned long long timestamp_t;
void get_local_time(char * string);
static timestamp_t get_timestamp();
void SIGINT_handler(int signa);

int main(int argc, char ** argv){
    int parser = 0;
    int parser_number=0;
    char ip_number[20];
    int i=0,j=0;
    int id=0,port=0;
    int query_fd=0;
    char queries_name[1000];
    char printformat[10000];
    char date_string[1000];
    int inputcheck[4]={0,0,0,0};

    while((parser = getopt(argc, argv, "i:a:p:o:")) != -1)   
    {  
        switch(parser)  
        {  
            case 'i':
            {
                if(inputcheck[0]>=1){
                    perror("Same input twice.\n");
                    return -1;
                }
                sscanf(optarg,"%d",&id);
                inputcheck[0]++;
                break;
            }
            case 'a':
            {
                if(inputcheck[1]>=1){
                    perror("Same input twice.\n");
                    return -1;
                }
                sprintf(ip_number,"%s",optarg);
                inputcheck[1]++;  
                break;
            }  
            case 'p':
            {
                if(inputcheck[2]>=1){
                    perror("Same input twice.\n");
                    return -1;
                }
                sscanf(optarg,"%d",&port);
                inputcheck[2]++;
                break; 
            }
            case 'o':
            {
                if(inputcheck[3]>=1){
                    perror("Same input twice.\n");
                    return -1;
                }
                sprintf(queries_name,"%s",optarg);
                query_fd = open(optarg,O_RDONLY,0666);
                if(query_fd == -1){
                    perror("Open error");
                    exit(EXIT_FAILURE);
                }
                inputcheck[3]++;  
                break;
            }
            default:
            {
                perror("The command line arguments are missing/invalid.\n");
                return -1;
            }
            break;
        }
        parser_number++; 
    }
    if(parser_number!=4){
        perror("Too many/less arguments.\n");
        return -1;
    }

    signal(SIGINT,SIGINT_handler);
    int end=lseek(query_fd,0,SEEK_END);
    int curr=lseek(query_fd,0,SEEK_SET);

    char c;
    i=0;
    do{
        if(read(query_fd,&c,1)==-1){
            perror("Read Error");
            exit(EXIT_FAILURE);
        }
        if(c=='\n'){
            i++;
        }
        curr=lseek(query_fd,0,SEEK_CUR);
    }
    while(curr<end);

    sprintf(printformat,"Row size:%d\n",i);
    write(1,printformat,strlen(printformat));
    char query[i][4096];
    char str[4096];
    i=0;
    int id_check=1,skip=0;
    int temp=0;
    curr=lseek(query_fd,0,SEEK_SET);
    do{
        j=0;
        id_check=1;
        do{
            if(read(query_fd,&c,1)==-1){
                perror("Read Error");
                exit(EXIT_FAILURE);
            }
            if(c==' ' && id_check==1){
                str[j]='\0';
                sscanf(str,"%d",&temp);
                if(temp==id){
                    j=-1;
                }
                else{
                    skip=1;
                }
                id_check=0;
            }
            if(skip!=1){
                str[j]=c;
                j++;
            }
            if(j==-1){
                j++;
            } 
        }
        while(c!='\n');
        str[j-1]='\0';
        if(skip!=1){
            strcpy(query[i],str);
            i++;
        }
        skip=0;
        curr=lseek(query_fd,0,SEEK_CUR);
    }
    while(curr<end);
    int k;
    for(k=0;k<i;k++){
        get_local_time(date_string);
        sprintf(printformat,"%s Queries:%s\n",date_string,query[k]);
        write(1,printformat,strlen(printformat));
    }

    struct sockaddr_in serv_addr;
    int socket_fd=0;      

    if ((socket_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1){ 
        perror("Socket error\n"); 
        return -1; 
    } 
   
    serv_addr.sin_family = AF_INET; 
    serv_addr.sin_port = htons(port); 
    if(inet_pton(AF_INET, ip_number, &serv_addr.sin_addr)== -1){ 
        perror("Address is invalid/not supported"); 
        return -1; 
    } 
   
    get_local_time(date_string);
    sprintf(printformat,"%s Client %d connecting to %s:%d\n",date_string,id,ip_number,port);
    write(1,printformat,strlen(printformat));

    if(connect(socket_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr))== -1){ 
        perror("Connection Failed"); 
        return -1; 
    } 

    get_local_time(date_string);
    sprintf(printformat,"%s Client %d connected and sending query %s\n",date_string,id,query[0]);
    write(1,printformat,strlen(printformat));

    if(send(socket_fd,&i,sizeof(int),0) == -1){
        perror("Send error.");
        return -1;
    }
    int c1=0;
    for(c1=0;c1<i;c1++){
        sprintf(printformat,"%s",query[c1]);
        int size_of_str=strlen(printformat);
        if(send(socket_fd,&size_of_str,sizeof(int),0) == -1){
            perror("Send error.");
            return -1;
        }

        if(send(socket_fd,printformat,strlen(printformat),0) == -1){
            perror("Send error.");
            return -1;
        }
        timestamp_t dataset_start = get_timestamp();
        timestamp_t dataset_end;
        char recv_string[10000];
        get_local_time(date_string);
        int ret=0;
        int record=0;
        int size_of_string=0;
        while(1){
            ret=recv(socket_fd, &size_of_string, sizeof(int), 0);
            if(ret == -1){
                perror("Recv error.");
                return -1;
            }
            ret=recv(socket_fd, recv_string, size_of_string, 0);
            if(ret == -1){
                perror("Recv error.");
                return -1;
            }
            recv_string[size_of_string]='\0';
            if(strstr(recv_string,"Last message")!=NULL){
                dataset_end = get_timestamp();
                break;
            }
            sprintf(printformat,"%s Server’s response to Client %d: %s\n",date_string,id,recv_string);
            write(1,printformat,strlen(printformat));
            record++;
        }    
        double read_time = (dataset_end - dataset_start) / 1000000.0L;
        sprintf(printformat,"%s Server’s response to Client %d is %d records, and arrived in %lf seconds\n",date_string,id,record,read_time);
        write(1,printformat,strlen(printformat));

    }
    
    exit(EXIT_SUCCESS);
}

void get_local_time(char * string){
    time_t time_type = time(NULL);
    struct tm *time_tm = localtime(&time_type);
    strftime(string,30,"%F %T",time_tm);
}

static timestamp_t get_timestamp(){
    struct timeval now;
    gettimeofday (&now, NULL);
    return  now.tv_usec + (timestamp_t)now.tv_sec * 1000000;
}

void SIGINT_handler(int signa){
    write(1,"SIGINT catched. Terminating...\n",sizeof("SIGINT catched. Terminating...\n")+1);
    exit(EXIT_FAILURE);
}
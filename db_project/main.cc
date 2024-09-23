#include <pthread.h>
#include <cstring>
#include <iostream>

#include "db/include/bpt.h"
#include "db/include/file.h"
#include "db/include/db.h"
// MAIN

int main( int argc, char ** argv ) {

    puts("START");
    
    auto it = open_table("gisub.db");
    init_db(20);
    
    char t[300]; uint16_t val_size;
    page_t p;
    for(int i = 1; i <= 600; i++)
    {
        sprintf(t, "bigbewewjgioejwojigjiwejiowqddqwwqig record #%d!!", i);
        int len = strlen(t);
        db_insert(it, i, t, len);
    }

    for(int i = 1; i <= 600; i += 1)
    {
        printf("Delete #%d\n", i);
        if(i == 536)
        {
            puts("dege");
        }
        else db_delete(it, i);
    }
    
    std::vector<int64_t> keys;
    std::vector<char*> values;
    std::vector<uint16_t> val_sizes;

    db_scan(it, 1, 12001, &keys, &values, &val_sizes);

    for(int i = 0; i < keys.size(); i++)
    {
        std::cout << keys[i] << "/" << values[i] << "/"  << val_sizes[i] << std::endl;
        delete values[i];
    }
    puts("Safety off");
    return 0;


/*

    char * input_file;
    FILE * fp;
    node * root;
    int input, range2;
    char instruction;
    char license_part;

    root = NULL;
    verbose_output = false;

    if (argc > 1) {
        order = atoi(argv[1]);
        if (order < MIN_ORDER || order > MAX_ORDER) {
            fprintf(stderr, "Invalid order: %d .\n\n", order);
            usage_3();
            exit(EXIT_FAILURE);
        }
    }

    license_notice();
    usage_1();  
    usage_2();

    if (argc > 2) {
        input_file = argv[2];
        fp = fopen(input_file, "r");
        if (fp == NULL) {
            perror("Failure  open input file.");
            exit(EXIT_FAILURE);
        }
        while (!feof(fp)) {
            fscanf(fp, "%d\n", &input);
            root = insert(root, input, input);
        }
        fclose(fp);
        print_tree(root);
    }

    printf("> ");
    while (scanf("%c", &instruction) != EOF) {
        switch (instruction) {
        case 'd':
            scanf("%d", &input);
            root = db_delete(root, input);
            print_tree(root);
            break;
        case 'i':
            scanf("%d", &input);
            root = insert(root, input, input);
            print_tree(root);
            break;
        case 'f':
        case 'p':
            scanf("%d", &input);
            find_and_print(root, input, instruction == 'p');
            break;
        case 'r':
            scanf("%d %d", &input, &range2);
            if (input > range2) {
                int tmp = range2;
                range2 = input;
                input = tmp;
            }
            find_and_print_range(root, input, range2, instruction == 'p');
            break;
        case 'l':
            print_leaves(root);
            break;
        case 'q':
            while (getchar() != (int)'\n');
            return EXIT_SUCCESS;
            break;
        case 't':
            print_tree(root);
            break;
        case 'v':
            verbose_output = !verbose_output;
            break;
        case 'x':
            if (root)
                root = destroy_tree(root);
            print_tree(root);
            break;
        default:
            usage_2();
            break;
        }
        while (getchar() != (int)'\n');
        printf("> ");
    }
    printf("\n");*/
    return EXIT_SUCCESS;
}

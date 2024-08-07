from elasticsearch import Elasticsearch as esserver
esclient = esserver('http://localhost:9200')
from cluster_pseudo_RF import QE_kNN

def searchTxt(pathInput, indexName):

  for card_R in [10,25,50,75,100]:
    for e in [10,25,50,75,100]:
      for Lambda in [range(0.1,1,0.1)]:

        file = open(pathInput, 'r')
        fileO = open(f"C:/Users/Utente/OneDrive/Universita/2023-24/IR/Progetto/Results/Test/results_{card_R}_{e}_{Lambda}_minoccurrences10.txt", 'w')

        while True:
          content=file.readline()
          if("<num>" in content):
            qIndx = content.removeprefix("<num> Number: ").strip()
          elif("<title>" in content):
            content = content.removeprefix("<title>").strip()
            if(len(content)==0):
              content=file.readline().strip()
            print("\nquery num:", qIndx, ", content:", content)
            response = QE_kNN(esclient, indexName, 528155, content, card_R, 5, e, Lambda)
            i = 1
            for hit in response['hits']['hits']:          
              st =f"{qIndx} Q0 {hit["_id"]} {i} {hit["_score"]} {hit["_index"]} \n"
              i +=1
              fileO.write(st) #numero query, indice     
          elif not content:
            break

        file.close()
        fileO.close()

searchTxt("C:/Users/Utente/OneDrive/Universita/2023-24/IR/Progetto/test.txt", "robust")
import sys
import os
import re

def main():

  file_path = "data/data.tsv"
  output_path = "data/result.tsv"

  if not os.path.isfile(file_path):
    print("File path {} does not exist. Exiting...".format(file_path))
    sys.exit()
    
  result = []
  previousResult = []

  try:
    fp = open(file_path, encoding="utf-16-le")
    output = open(output_path, 'a', newline='\n', encoding='utf-8')

    current_line = ""
    current_ind = 0

    for x in fp:
      arr_line = x.split('\t')

      if(current_ind > 0):
        try:
          ind = int(arr_line[0])
          result = arr_line
          current_line = x
          current_ind += 1
        except:
          result = result + arr_line
          current_line = current_line + " " + x
      else:
        current_ind += 1

      if previousResult:
        if(int(previousResult[0]) == int(result[0])):
          previousResult = result
      else:
        previousResult = result
        
      if(result and int(result[0]) > int(previousResult[0])):
        formated_tsv = processList(previousResult)
        print(formated_tsv)
        output.write(formated_tsv)
        previousResult = result

  finally:
    final_value_tsv = processList(result)
    output.write(final_value_tsv)
    fp.close()
    output.close()

def processList(ls):

  ls = list(map(lambda x: '\'' + x.strip() + '\'' if if_escape(x) else x, ls))

  ls = list(map(str.strip,ls))

  if not ls[3].isdigit():
    ls[3] = re.sub('[^0-9]','', ls[3])

  if len(ls) > 5:
    if ls[2] == ls[3]:
      ls.pop(3)
    else:
      ls[2] = ls[2] + ' ' + ls[3]
      ls.pop(3)
 
  result = "\t".join(ls) 
  return result

def if_escape(str):
  regex = re.compile('[@_!#$%^&*()<>?/\|}{~:]')
  
  if(regex.search(str) == None):
    return False
  else:
    return True

if __name__ == '__main__':
  main()

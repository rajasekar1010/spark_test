list=[1,2,1,2,3,4,3,4]
i=0
while i < len(list):
    j=i+1
    while j < len(list):
        if list[i] == list[j]:
            list.remove(list[j])
        else:
            j+=1
    i+=1
print(list)
import string
import random

def gen_avg_data(limit):
    alphabet = string.ascii_lowercase
    data = []
    for i in range(limit):
        idx = random.randint(0, len(alphabet)-1)
        cur_char = idx
        cur_freq = random.randint(1, 100)
        data.append(str(cur_char)+" "+str(cur_freq))
    return data


def write_list(path,data):
    with open(path,'w') as w:
        for x in data:
            w.write(x+'\n')

def gen_inverteddata(limit):
    alphabet = string.ascii_lowercase
    data = []
    for i in range(limit):
        idx = random.randint(1, 100)
        n = random.randint(4, 20)

        line =str(idx)+":"
        for j in range(n):
            idx = random.randint(0, len(alphabet)-1)
            line += " "+str(alphabet[idx])
        data.append(line.strip())
    return data


def gen_join_data(limit):
    user=[]
    for i in range(limit):
        user.append((random.randint(1,20),"user-data-"+str(random.randint(1,1000))))

    with open('user.txt', 'w') as w:
        for x in user:
            w.write(str(x[0])+" "+x[1]+"\n")

    comments=[]
    for i in range(limit*limit):
        comments.append((random.randint(1,20),"comments-"+str(random.randint(1,1000))))

    with open('comments.txt','w') as w:
        for x in comments:
            w.write(str(x[0])+" "+x[1]+"\n")


def get_random_text(length):
    alphabet = string.ascii_lowercase
    res =''
    for i in range(length):
        res+=alphabet[random.randint(0,len(alphabet)-1)]
    return res

def gen_user_data(limit):
    user=[]
    for i in range(limit):
        msg =str(random.randint(1,20)) + " firstName: "+get_random_text(5)+\
        " lastName: "+get_random_text(5)+\
        " email: "+get_random_text(5)+"@gmail.com" + " reputation: "+str(random.randint(1,100))
        user.append(msg)

    with open("users.txt","w") as w:
        for x in user:
            w.write(x+"\n")


def customInputData(limit):
    data=[]
    for x in range(limit):
        data.append((random.randint(1,20), random.randint(100,200)))

    with open('custom-data.txt','w') as w:
        for x in data:
            w.write(str(x[0])+" "+str(x[1])+'\n')


def main():
    # data = gen_avg_data(10000)
    # w_path = 'avg_data.txt'

    # write_list(w_path, data)

    limit = 100
    # inverted_path = 'inverted.txt'
    # inver_data = gen_inverteddata(limit)

    # write_list(inverted_path, inver_data)

    gen_join_data(limit)

    gen_user_data(limit)
    # customInputData(limit)


if __name__ =="__main__":
    main()

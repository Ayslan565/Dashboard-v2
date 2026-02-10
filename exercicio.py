import sys
import os
print("Escreva um número de 0-9")
escolha = int(input("Escreva o número: "))
if escolha ==0:
       
        print("Número inválido!")

elif escolha ==1:
        print("Você escolheu 1")
elif escolha ==2:
        print("Você escolheu 2")
elif escolha ==3:
        print("Você escolheu 3")
    
else:
        print("Número inválido!")

new_tuple = ('x', 'y','z') 

new_tuple = new_tuple + ('w',)
print(new_tuple)
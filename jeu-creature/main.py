from Case import Case
from Creature import Creature
from Jeu import Jeu

plateau_4x4 = [Case(x, y) for x in range(4) for y in range(4)]
mage = Creature("Mage noir", Case(0, 0))
scaven = Creature("Scaven", Case(3, 3))
listes_creatures = [mage, scaven]

print("++ PLATEAU DE CREATURES ++")
for x in range(4):
        for y in range(4):
            if scaven.position.x == x and scaven.position.y == y:
                print(" S ", end="")
            elif mage.position.x == x and mage.position.y == y:
                print(" M ", end="")
            else:
                print(" . ", end="")
        print()

print("++ PLATEAU DE CREATUREs ++")
print()
jeu = Jeu(plateau_4x4, listes_creatures)

while jeu.continuerJeu:
    jeu._set_actif(mage)
    mage.choisirCible(jeu)
    jeu._set_actif(scaven)
    scaven.choisirCible(jeu)
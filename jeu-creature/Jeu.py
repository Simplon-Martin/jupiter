class Jeu:
    def __init__(self, listesDesCase, listeDesCreatures, tour=0):
        self.listesDesCase = listesDesCase
        self.listeDesCreatures = listeDesCreatures
        self.tour = tour
        self.actif = listeDesCreatures[0]
        self.continuerJeu = True

    def _get_actif(self):
        return self._actif

    def _set_actif(self, creature):
        for i in self.listeDesCreatures:
            if i == creature:
                self._actif = i

    actif = property(_get_actif, _set_actif)

    def estOccupee(self, case):
        occupee = False
        for creature in self.listeDesCreatures:
            if creature != self.actif:
                creature_adverse = creature
        if case.x == creature_adverse.position.x and case.y == creature_adverse.position.y:
            occupee = True
        return occupee

    def deplacer(self, creature, case):
        self.tour += 1
        creature._set_position(case)
        for creat in self.listeDesCreatures:
            if creat != self.actif:
                creature_adverse = creat
        if creature.position.x == creature_adverse.position.x and creature.position.y == creature_adverse.position.y:
            self.continuerJeu = False
            for x in range(4):
                for y in range(4):
                    if creature.position.x == x and creature.position.y == y:
                        print("*" + creature.nom[0].upper() + "*", end="")
                    else:
                        print(" . ", end="")
                print()
            print(self.actif.nom + " Gagne !")
            return creature
        elif self.continuerJeu:
            print(self.actif.nom + " se deplace !")
            for x in range(4):
                for y in range(4):
                    if creature.position.x == x and creature.position.y == y:
                        print(" " + creature.nom[0].upper() + " ", end="")
                    elif creature_adverse.position.x == x and creature_adverse.position.y == y:
                        print(" " + creature_adverse.nom[0].upper() + " ", end="")
                    else:
                        print(" . ", end="")
                print()
            print("Aucun adversaire en vue ! ")
            return None

    def __str__(self):
        return " joueur Gagnant : " + self.actif.nom

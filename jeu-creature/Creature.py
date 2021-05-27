from random import randrange


class Creature:
    def __init__(self, nom, position):
        self.nom = nom
        self.position = position

    def _get_position(self):
        return self._position

    def _set_position(self, position):
        self._position = position

    position = property(_get_position, _set_position)

    def choisirCible(self, jeu):
        voisine = ""
        cases_possible = self.position.adjacentes(jeu)
        for cp in cases_possible:
            if jeu.estOccupee(cp):
                voisine = cp
                break
            else:
                voisine = cases_possible[randrange(len(cases_possible))]
        jeu.deplacer(jeu.actif, voisine)

class Case:
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def adjacentes(self, jeu):
        cases_adjascentes = []
        for c in jeu.listesDesCase:
            if (abs(c.x - self.x) <= 1) and (abs(c.y - self.y) <= 1):
                if (c.x == self.x) and (c.y == self.y):
                    continue
                cases_adjascentes.append(c)
        return cases_adjascentes

    def __str__(self):
        return str(self.x) + " " + str(self.y)
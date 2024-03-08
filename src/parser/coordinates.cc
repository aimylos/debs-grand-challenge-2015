
#define X_500m            (5986)
#define X_250m            (X_500m/2)
#define X_MIN_CENTER      (-74913585)
#define X300_MIN          (X_MIN_CENTER - X_500m/2)
#define X300_MAX          (X300_MIN + 300*X_500m)
#define X600_MIN          (X_MIN_CENTER - X_250m/2)

#define Y_500m            (4491.556)  // :1000
#define Y_250m            (Y_500m/2)    // :1000
#define Y_MAX_CENTER      (41474937)
#define Y300_MAX          (Y_MAX_CENTER + Y_500m/2)
#define Y300_MIN          (Y300_MAX - 300*Y_500m)
#define Y600_MAX          (Y_MAX_CENTER + Y_250m/2)



bool invalidX(int coord)
{
  return coord<X300_MIN || coord>X300_MAX;
}



bool invalidY(int coord)
{
  return coord < Y300_MIN || coord > Y300_MAX;
}



int getCellX300(int coord)
{
  return (coord - X300_MIN) / X_500m;
}



int getCellY300(int coord)
{
  return (Y300_MAX - coord) / Y_500m;
}



int getCellX600(int coord)
{
  return (coord-X600_MIN) / X_250m;
}



int getCellY600(int coord)
{
  return (Y600_MAX - coord) / Y_250m;
}

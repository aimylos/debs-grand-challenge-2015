#ifndef DISKREADER_H_
#define DISKREADER_H_


class DiskReader
{
  private:
    static char *filename;

  public:
    /** Sets the input filename. */
    static void setFilename(char *filename);

    /** The disk reader thread start routine. */
    static void *thread_main(void *);
};


#endif /* DISKREADER_H_ */

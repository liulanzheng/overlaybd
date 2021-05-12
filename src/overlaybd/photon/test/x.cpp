int DevNull(void* x, int)
{
    return 0;
}

int (*pDevNull)(void*, int) = &DevNull;

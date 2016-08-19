(
    FOR /L %%A IN (1,1,500) DO (
      python trashdemo.py --logdir ./logs// --verbosity info
    )
) > trashdemobat.pylog 2>&1
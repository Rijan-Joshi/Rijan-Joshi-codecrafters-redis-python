areas = [
            106.67, 101.72, 69.79, 346.95, 251.09, 84.81, 291.74, 1571.79, 258.33,
            56.18, 80.26, 287.59, 1550.69, 554.23, 48.89, 254.3, 349.09, 207.75,
            129.88, 287.83, 149.68, 180.48, 93.23, 88.87, 208.54, 106.58, 141.56,
            128.74, 65.85, 79.03, 68.15, 54.55, 97.68, 89.59, 72.69, 70.84, 300.66,
            223.87, 437.12, 213.47, 489.38, 77.59, 110.38, 218.1, 99.5, 103.8,
            223.17, 109.73, 268.93, 218.1, 214.04, 51.5, 86.07, 314.62, 148.97,
            306.03, 96.77, 104.67, 118.53, 181.62, 132.72, 86.21, 83.72, 1790.32,
            59.98, 88.88, 80.5, 251.77, 745.69, 159.79, 610.06, 105.75, 84.71,
            184.22, 116.05, 714.18, 276.05, 121.92, 182.51, 220.72, 96.24, 93.42,
            99.79, 84.86, 67.46, 91.59, 101.73, 75.9, 62.66, 134.46, 116.22, 168.45,
            111.73, 89.26, 128.79, 94.29, 66.72, 104.79, 106.25, 61.83
        ]

areas_below_hundred = list(filter(lambda x: x < 100, areas))
print(areas_below_hundred)
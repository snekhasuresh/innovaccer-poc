<?php
global $filter_keys;
function car_search_filter_shortcode()
{
    // Sample data arrays (you can replace these with dynamic data later)
    $price_ranges = [
        'all' => 'All',
        '0-40K' => '0-40K',
        '40-60K' => '40-60K',
        '60-90K' => '60-90K',
        '90-120K' => '90-120K',
        '120-150K' => '120-150K',
        '150-200K' => '150-200K',
        '200-300K' => '200-300K',
        '300-400K' => '300-400K',
        '400-600K' => '400-600K',
        '600-3000K' => '600-3000K'
    ];
    $body_types = [
        'all' => 'All',
        'sedan' => 'Sedan',
        'hatchback' => 'Hatchback',
        'mpv' => 'MPV',
        'suv' => 'SUV',
        'pickup' => 'Pickup',
        'wagon' => 'Wagon',
        'coupe' => 'Coupe',
        'convertible' => 'Convertible',
        'commercial' => 'Commercial'
    ];
    $segments = [
        'all' => 'All',
        'a-segment' => 'A-Segment',
        'b-segment' => 'B-Segment',
        'c-segment' => 'C-Segment',
        'd-segment' => 'D-Segment',
        'e-segment' => 'E-Segment',
        'grand-tourer' => 'Grand Tourer',
        'luxury' => 'Luxury',
        'sports-car' => 'Sports Car',
        'super-car' => 'Super Car',
        'compact-executive' => 'Compact Executive',
        '4x4' => '4x4',
        '4x2' => '4x2'
    ];

    $transmissions = [
        'all' => 'All',
        'mt' => 'MT',
        'amt' => 'AMT',
        'cvt' => 'CVT',
        'dct' => 'DCT',
        'at' => 'AT',
        'mct' => 'MCT',
        'ev' => 'EV',
        'e-cvt' => 'E-CVT'
    ];

    $fuels = [
        'all' => 'All',
        'petrol' => 'Petrol',
        'diesel' => 'Diesel',
        'petrol-hybrid' => 'Petrol Hybrid',
        'diesel-hybrid' => 'Diesel Hybrid',
        'ev' => 'Electric Vehicle (EV)'
    ];


    // Additional filters that will go inside the "More Options" section
    $additional_filters = [
        'colors' => [
            'black' => 'Black',
            'white' => 'White',
            'red' => 'Red',
            'blue' => 'Blue'
        ],
        'drivetrain' => [
            'all' => 'All',
            'awd' => 'AWD',
            'fwd' => 'FWD',
            'rwd' => 'RWD'
        ],
        'seats' => [
            'all' => 'All',
            '2' => '2 Seats',
            '4' => '4 Seats',
            '5' => '5 Seats',
            '7' => '7+ Seats'
        ],
        'features_and_specs' => [
            'all' => 'All',
            'cruise_control' => 'Cruise Control',
            'hill_start_assist' => 'Hill Start Assist',
            'sunroof' => 'Sunroof',
            'paddle_shift' => 'Paddle Shift',
            'sunshade' => 'Sunshade',
            'car_bluetooth' => 'Car Bluetooth',
            'isofix' => 'ISOFIX',
            'abs_system' => 'ABS System'
        ]
    ];


    ob_start();
?>
    <div class="car-search-filters">
        <h2 class="heading-search-filters wa-title-text">Search Filters</h2>

        <!-- Price Filter -->
        <div class="filter-group-container">
            <div class="filter-group">
                <label>Price:</label>
                <ul>
                    <?php foreach ($price_ranges as $key => $label): ?>
                        <li><button class="filter-option" data-filter="price_range" data-value="<?php echo $key; ?>"><?php echo $label; ?></button></li>
                    <?php endforeach; ?>
                </ul>
            </div>

            <!-- Body Type Filter -->
            <div class="filter-group">
                <label>Body Type:</label>
                <ul>
                    <?php foreach ($body_types as $key => $label): ?>
                        <li><button class="filter-option" data-filter="body_type" data-value="<?php echo $key; ?>"><?php echo $label; ?></button></li>
                    <?php endforeach; ?>
                </ul>
            </div>

            <!-- Segment Filter -->
            <div class="filter-group">
                <label>Segment:</label>
                <ul>
                    <?php foreach ($segments as $key => $label): ?>
                        <li><button class="filter-option" data-filter="segment" data-value="<?php echo $key; ?>"><?php echo $label; ?></button></li>
                    <?php endforeach; ?>
                </ul>
            </div>

            <!-- Transmission Filter -->
            <div class="filter-group">
                <label>Transmission:</label>
                <ul>
                    <?php foreach ($transmissions as $key => $label): ?>
                        <li><button class="filter-option" data-filter="transmission" data-value="<?php echo $key; ?>"><?php echo $label; ?></button></li>
                    <?php endforeach; ?>
                </ul>
            </div>

            <!-- Fuel Filter -->
            <div class="filter-group">
                <label>Fuel:</label>
                <ul>
                    <?php foreach ($fuels as $key => $label): ?>
                        <li><button class="filter-option" data-filter="fuel_type" data-value="<?php echo $key; ?>"><?php echo $label; ?></button></li>
                    <?php endforeach; ?>
                </ul>
            </div>

            <!-- More Options Section -->
            <div class="more-options-section" style="display: none;"> <!-- Hidden by default -->

                <!-- Seats Filter -->
                <!-- Driveline Filter -->
                <div class="filter-group">
                    <label>Drivetrain:</label>
                    <ul>
                        <?php foreach ($additional_filters['drivetrain'] as $key => $label): ?>
                            <li><button class="filter-option" data-filter="drivetrain" data-value="<?php echo $key; ?>"><?php echo $label; ?></button></li>
                        <?php endforeach; ?>
                    </ul>
                </div>

                <!-- Features & Specs Filter -->
                <div class="filter-group">
                    <label>Features & Specs:</label>
                    <ul>
                        <?php foreach ($additional_filters['features_and_specs'] as $key => $label): ?>
                            <li><button class="filter-option" data-filter="features_and_specs" data-value="<?php echo $key; ?>"><?php echo $label; ?></button></li>
                        <?php endforeach; ?>
                    </ul>
                </div>

                <!-- Color Filter (Example additional filter) -->
                <div class="filter-group">
                    <label>Seat:</label>
                    <ul>
                        <?php foreach ($additional_filters['seats'] as $key => $label): ?>
                            <li><button class="filter-option" data-filter="seats" data-value="<?php echo $key; ?>"><?php echo $label; ?></button></li>
                        <?php endforeach; ?>
                    </ul>
                </div>

            </div>

            <!-- More Options Toggle Button -->
            <div class="more-options-container">
                <div class="line"></div>
                <button class="more-options-toggle">
                    More Options <span style="margin-left: 5px;"><i class="fas fa-chevron-down"></i></span>
                </button>
                <div class="line"></div>
            </div>

        </div>
        <div class="selected-filters" id="selectedFilters" style="display: none;">
            <div style="display: flex;">
                <h4>Selected Filters:</h4>
                <div id="selectedFiltersContainer"></div>
            </div>
            <button id="resetFilters">Reset All</button>
        </div>

    </div>
    <script>
        document.addEventListener('DOMContentLoaded', function() {
            const allButtonClass = 'highlight-yellow';
            const activeButtonClass = 'highlight-dark-yellow';

            // Initialize all "All" filters as selected by default
            document.querySelectorAll('.filter-option[data-value="all"]').forEach(function(button) {
                button.classList.add(allButtonClass);
            });

            // Object to store selected values by filter type (e.g., price, brand, etc.)
            const selectedFilters = {};

            // Function to update the selected filters display
            // Update the selected filters display
            function updateSelectedFiltersDisplay() {
                const container = document.getElementById('selectedFiltersContainer');
                container.innerHTML = ''; // Clear the container

                // Loop through selected filters
                for (const filterType in selectedFilters) {
                    const filterValues = selectedFilters[filterType];
                    if (filterValues.length > 0) {
                        // Create a div to hold the filter values
                        const valuesContainer = document.createElement('div');
                        filterValues.forEach(value => {
                            const filterItem = document.createElement('div');
                            filterItem.textContent = value; // Display the filter value

                            // Create delete button with Font Awesome icon
                            const deleteButton = document.createElement('button');
                            deleteButton.classList.add('delete-filter');
                            deleteButton.innerHTML = '<i class="fa fa-times" style="font-size: 9px;"></i>'; // Add Font Awesome icon

                            // On click, remove the specific filter value
                            deleteButton.onclick = function() {
                                // Remove this specific value from selected filters
                                selectedFilters[filterType] = selectedFilters[filterType].filter(v => v !== value);
                                // Reset buttons in the UI
                                const buttons = document.querySelectorAll(`[data-filter="${filterType}"][data-value="${value}"]`);
                                buttons.forEach(function(button) {
                                    button.classList.remove(activeButtonClass);
                                    button.classList.remove(allButtonClass);
                                });
                                // Update the display
                                updateSelectedFiltersDisplay();
                            };

                            filterItem.appendChild(deleteButton);
                            valuesContainer.appendChild(filterItem); // Add filter item to values container
                        });
                        container.appendChild(valuesContainer); // Add values container to main container
                    }
                }

                // Toggle the visibility of the "selected-filters" section based on if any filters are selected
                if (Object.values(selectedFilters).some(filterValues => filterValues.length > 0)) {
                    document.getElementById('selectedFilters').style.display = 'flex';
                } else {
                    document.getElementById('selectedFilters').style.display = 'none';
                }
            }

            // Add click event listeners to filter buttons
            document.querySelectorAll('.filter-option').forEach(function(button) {
                button.addEventListener('click', function() {
                    const filterType = button.getAttribute('data-filter');
                    const filterValue = button.getAttribute('data-value');

                    // Ensure we initialize the filter type
                    if (!selectedFilters[filterType]) {
                        selectedFilters[filterType] = [];
                    }

                    // Handle "All" button click
                    if (filterValue === 'all') {
                        // Deselect all other buttons for this filter type
                        document.querySelectorAll(`[data-filter="${filterType}"]`).forEach(function(b) {
                            b.classList.remove(activeButtonClass, allButtonClass);
                        });

                        // Highlight the "All" button and reset selected filters for this type
                        button.classList.add(allButtonClass);
                        selectedFilters[filterType] = [];

                    } else {
                        // Deselect the "All" button when specific filters are selected
                        document.querySelector(`[data-filter="${filterType}"][data-value="all"]`).classList.remove(allButtonClass);

                        // Toggle active state for the clicked button
                        button.classList.toggle(activeButtonClass);

                        // Add or remove the filter value from selectedFilters
                        if (button.classList.contains(activeButtonClass)) {
                            if (!selectedFilters[filterType].includes(filterValue)) {
                                selectedFilters[filterType].push(filterValue);
                            }
                        } else {
                            // Remove the filter value if it was deselected
                            const index = selectedFilters[filterType].indexOf(filterValue);
                            if (index > -1) {
                                selectedFilters[filterType].splice(index, 1);
                            }
                        }
                    }

                    updateSelectedFiltersDisplay();
                });
            });

            // More options toggle
            document.querySelector('.more-options-toggle').addEventListener('click', function() {
                const moreOptionsSection = document.querySelector('.more-options-section');
                moreOptionsSection.style.display = moreOptionsSection.style.display === 'none' ? 'block' : 'none';
                this.querySelector('span i').classList.toggle('fa-chevron-up');
                this.querySelector('span i').classList.toggle('fa-chevron-down');
            });

            // Reset filters
            document.getElementById('resetFilters').addEventListener('click', function() {
                // Reset selected filters object
                for (const filterType in selectedFilters) {
                    selectedFilters[filterType] = [];
                    document.querySelectorAll(`[data-filter="${filterType}"]`).forEach(function(button) {
                        button.classList.remove(activeButtonClass);
                        button.classList.remove(allButtonClass);
                    });
                }

                // Select "All" for all filter types
                document.querySelectorAll('.filter-option[data-value="all"]').forEach(function(button) {
                    button.classList.add(allButtonClass);
                });

                updateSelectedFiltersDisplay();
            });
        });
    </script>


    <style>
        #selectedFiltersContainer {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            margin-bottom: 10px;
        }

        #selectedFiltersContainer div {
            display: flex;
            align-items: center;
            position: relative;
        }

        .car-search-filters {
            font-family: Arial, sans-serif;
            margin-bottom: 20px;
            margin-top: 43px;
        }



        .heading-search-filters {
            font-size: 26px;
            line-height: 32px;

            font-weight: bold;
            color: black;
        }

        .filter-group {
            margin-bottom: 10px;
            display: flex;
            font-size: 14px;
            font-weight: 500;

        }

        .filter-group-container {
            margin-top: 30px;
        }

        .filter-group label {
            flex: 0 0 124px;
            margin-right: 10px;
            text-align: left;
            font-size: 14px;
            font-weight: 500;
            /* Align the label text to the right */
        }


        .filter-group ul {
            list-style: none;
            padding: 0;
            margin: 0;
            display: flex;
            flex-wrap: wrap;
        }

        .filter-group ul li {
            margin-right: 8px;
            /* Reduced margin */
            margin-bottom: 8px;
        }

        .filter-group button {

            /* Reduced padding to make buttons smaller */
            font-size: 14px;
            font-family: "roboto";
            /* Reduced button text size */
            background: none;

            border: none;
            cursor: pointer;
            border-radius: 5px;

            padding-left: 11px;
            padding-right: 11px;
            font-weight: 400;
            /* Reduced button height */
        }

        .filter-group button.highlight-yellow {
            display: block;
            background: #32d0c6;
            color: #fff;
        }

        .filter-group button.highlight-yellow:hover,
        .filter-group button.highlight-dark-yellow:hover {
            background-color: #32d0c6;
            color: white;
        }

        .filter-group button.highlight-dark-yellow {
            background-color: #32D0C6;
            color: black;
        }

        .filter-group button:hover {
            background-color: #f5f5f5;
        }


        .more-options-container {
            display: flex;
            justify-content: center;
            align-items: center;
            margin-top: -30px;
        }

        .more-options-toggle {
            cursor: pointer;
            padding: 4px 8px;
            font-size: 12px;
            background-color: white;
            border: 1px solid #ccc;
            width: 227px;
            height: 40px;
            margin-top: 39px;
            border-top: none;
            background-color: white;
            font-family: "Roboto";
            font-weight: 700;
            line-height: 20px;
            font-size: 14px;
            color: #595959;

        }


        .line {
            height: 0.5px;
            background-color: #ccc;
            flex-grow: 1;
        }

        .selected-filters {
            border: 1px solid #e0e0e0;
            padding: 10px;
            border-radius: 5px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            height: 63px;
            background-color: #f9f9f9;
            margin-top: 28px;
        }

        .selected-filters h4 {
            font-size: 16px;
            margin-right: 10px;
        }

        .selected-filters .filter-tag {
            background-color: #32D0C6;
            color: white;
            padding: 5px 10px;
            border-radius: 3px;
            margin-right: 5px;
            display: inline-flex;
            align-items: center;
            cursor: pointer;
        }

        .selected-filters .filter-tag span {
            margin-right: 5px;
        }

        .selected-filters .filter-tag .remove-filter {
            background-color: #ccc;
            color: #fff;
            padding: 2px 5px;
            border-radius: 50%;
            font-size: 10px;
            cursor: pointer;
        }

        #resetFilters {
            border: none;
            background-color: #F9F9F9;

            cursor: pointer;

        }
    </style>

<?php
    return ob_get_clean();
}
add_shortcode('car_search_filter', 'car_search_filter_shortcode');

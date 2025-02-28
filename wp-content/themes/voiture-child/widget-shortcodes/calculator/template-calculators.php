<?php

function populate_car_dropdown()
{
    global $wpdb;
//     $cache_key = 'car_brands';
//     $car_brands = get_transient($cache_key);

//     if (false === $car_brands) {

        $car_brands = get_terms(array(
            'taxonomy'   => 'listing_make',
            'parent'     => 0,
            'orderby'    => 'name',
            'order'      => 'ASC',
            'hide_empty' => false,
        ));
//         set_transient($cache_key, $car_brands, HOUR_IN_SECONDS);
//     }
?>
    <div class="dropdown-wrapper">
        <div class="dropdown main-dropdown">
            <div id="select-cars" class="btn-default dropdown-toggle multi-level-drop-down" data-toggle="dropdown">Silakan pilih mobil Anda<span class="caret"></span></div>
            <ul class="dropdown-menu brand-menu" style="display:none;">
                <?php

                foreach ($car_brands as $brand) {
                ?>
                    <li class="brand-data" data-brand-id="<?php echo esc_attr($brand->term_id) ?>">
                        <!-- <div class="brandData"> -->
                        <a href="#"><?php echo esc_html($brand->name) ?>

                        </a>
                        <div>
                            <span class="right-arrow">&#10095;</span>
                        </div>
                        <!-- </div> -->
                    </li>
                <?php
                }
                ?>
            </ul>
            <ul class="dropdown-menu model-menu" style="display:none;"></ul>
            <ul class="dropdown-menu variant-menu" style="display:none;"></ul>
        </div>
    </div>
<?php
}

function fuel_cost_calculator_shortcode()
{
    ob_start();
?>
    <div class="custom-template-container">
        <h1 class="wa-title-text sub-class">Pajak Tahunan
        </h1>
        <div class="input-section">
            <div class="input-group">
                <label for="select-cars">Pilih Mobil
</label>
                <?php
                populate_car_dropdown();
                ?>
            </div>
            <?php
            $fuel_types = get_terms(array(
                'taxonomy' => 'fuel-type',
                'hide_empty' => false,
            ));

            if (!empty($fuel_types) && !is_wp_error($fuel_types)) {
            ?>
                <div class="input-group">
                    <label for="fuel-type">ประเภทน้ำมัน</label>
                    <select id="fuel-type">
                        <!-- <option value="" disabled selected>Select a Fuel Type</option> -->
                        <?php
                        foreach ($fuel_types as $fuel_type) {
                            $oil_posts = get_posts(array(
                                'post_type' => 'oil',
                                'meta_query' => array(
                                    array(
                                        'key' => 'fuel_type',
                                        'value' => $fuel_type->term_id,
                                        'compare' => '='
                                    )
                                ),
                                'posts_per_page' => -1,
                            ));
                            foreach ($oil_posts as $oil_post) {
                                $oil_name = get_post_meta($oil_post->ID, 'oil_name', true);
                                $oil_price = get_post_meta($oil_post->ID, 'oil_price', true)
                        ?>
                                <option value="<?php echo esc_attr($fuel_type->term_id . '-' . $oil_name); ?>"
                                    data-price="<?php echo esc_attr($oil_price); ?>">
                                    <?php echo strtoupper(esc_html($fuel_type->name . ' - ' . $oil_name)); ?>
                                </option>
                        <?php
                            }
                        }
                        ?>
                    </select>
                </div>
            <?php
            }
            ?>

            <div class="input-group">
                <label for="fuel-consumption">อัตราการสิ้นเปลืองน้ำมัน (ลิตร/100 กม.)</label>
                <input type="text" id="fuel-consumption" value="6.7">
            </div>

            <div class="input-group">
                <label for="fuel-cost-fuel-price">ราคาน้ำมัน (บาท)</label>
                <input type="text" id="fuel-cost-fuel-price" class="fuel-cost-fuel-price" value="RM 2.05">
            </div>

            <div class="input-group">
                <label for="year-distance">ระยะทางต่อปี (กม.)</label>
                <input type="text" id="year-distance" value="20,000">
            </div>
        </div>
        <div class="result-section">
            <div class="result-card">
                <p>ค่าน้ำมันต่อปี</p>
                <h2 id="yearly-fuel-payment">THB 50,619</h2>
            </div>
        </div>
    </div>
    <script>
        jQuery(document).ready(function($) {
            //calculale yearly Fuel payment -> fuel loan calculator
            function calculateYearlyFuelPayment() {

                const fuelConsumption = parseFloat($("#fuel-consumption").val());
                const fuelPrice = parseFloat($("#fuel-cost-fuel-price").val().replace(/THB\s*/, '')); // Remove THB
                const yearDistance = parseFloat(
                    $("#year-distance").val().replace(/,/g, "")
                );
                console.log('fuelConsumption', fuelConsumption, fuelPrice, yearDistance);

                if (!isNaN(fuelConsumption) && !isNaN(fuelPrice) && !isNaN(yearDistance)) {
                    const yearlyPayment = (
                        yearDistance *
                        (fuelConsumption / 100) *
                        fuelPrice
                    );
                    console.log('yearlyPayment', yearlyPayment);
                    $("#yearly-fuel-payment").text("THB " + Math.floor(yearlyPayment).toLocaleString());
                } else {
                    $("#yearly-fuel-payment").text("THB 0");
                }
            }

            $("#fuel-consumption").on("input change", function() {
                calculateYearlyFuelPayment();
            });

            $("#fuel-consumption, #fuel-cost-fuel-price, #year-distance, #fuel-type").on(
                "input change",
                calculateYearlyFuelPayment
            );
            $("#select-cars").on("change", function() {
                calculateYearlyFuelPayment();
            });

            // Initial calculation to show default values
            calculateYearlyFuelPayment();

            var selectElement = $("#fuel-type");
            var placeholderOption = selectElement.find('option[value=""]');

            //show fuel price based on the fuel type
            $("#fuel-type").on("change", function() {
                var selectedOption = $(this).find("option:selected");
                var price = selectedOption.data("price");

                if (price) {
                    $("#fuel-cost-fuel-price").val("THB " + price);
                } else {
                    $("#fuel-cost-fuel-price").val("THB 0");
                }
                calculateYearlyFuelPayment();
            });
			
			 // Set initial fuel price based on the first option
            $("#fuel-type").ready(function() {
                var initialPrice = $("#fuel-type option:first").data("price");
                if (initialPrice) {
                  $("#fuel-cost-fuel-price").val("THB " + initialPrice);
                }
                calculateYearlyFuelPayment();
            });
        });
    </script>
<?php
    return ob_get_clean();
}
add_shortcode('calculator_ui', 'fuel_cost_calculator_shortcode');

function road_tax_calculator_shortcode()
{
    ob_start();
?>
    <div class="custom-template-container">
        <h1 class="wa-title-text sub-class">Simulasi Asuransi Mobil</h1>

        <div class="input-section">
            <div class="input-group">
                <label for="select-cars">Pilih Mobil
</label>
                <?php
                populate_car_dropdown();
                ?>
            </div>
            <div class="input-group">
                <label for="engine-capacity">Harga Mobil(Rp)</label>
                <input type="text" id="road-engine-capacity" value="1200">
            </div>
        </div>
        <div class="result-section">
            <div class="result-card">
                <p>ภาษีประจำปี 6 ปีแรก</p>
                <h2 id="road-tax-result">THB 1,200</h2>
            </div>
        </div>
    </div>
    <script>
        jQuery(document).ready(function($) {
			console.log('Road tax calculator initialized');

			function calculateTax() {
				var engineCapacity = parseFloat($('#road-engine-capacity').val());

					if (isNaN(engineCapacity) || engineCapacity <= 0) {
						$('#road-tax-result').text('--');
						return;
					}

					let tax = 0;

					if (engineCapacity <= 600) {
						tax = engineCapacity * 0.50;
					} else if (engineCapacity <= 1800) {
						tax = 600 * 0.50 + (engineCapacity - 600) * 1.50;
					} else {
						tax = 600 * 0.50 + 1200 * 1.50 + (engineCapacity - 1800) * 4.00;
					}

					let taxAmount = Math.round(tax);  // Round to nearest integer
					taxAmount = taxAmount.toLocaleString();  // Format with commas

				console.log(`Engine Capacity: ${engineCapacity}, Tax: ${taxAmount}`);
				$('#road-tax-result').text('THB ' + taxAmount);
			}

			$('#road-engine-capacity').on('input', calculateTax);

			$('#select-cars').on('change', function() {
				var selectedCar = $(this).find('option:selected');
				var engineCapacity = selectedCar.data('engine-capacity');

				if (engineCapacity) {
					$('#road-engine-capacity').val(engineCapacity);
					console.log(`Selected car engine capacity: ${engineCapacity}`);
					calculateTax();
				} else {
					console.log('Engine capacity not available, resetting to default.');
					$('#road-engine-capacity').val(1200);
					calculateTax();
				}
			});

			// Trigger initial calculation
			calculateTax();
		});
    </script>
<?php
    return ob_get_clean();
}
add_shortcode('road_tax_calculator_ui', 'road_tax_calculator_shortcode');

function car_loan_calculator_shortcode()
{
    ob_start();
?>
    <div class="custom-template-container">
        <h1 class="wa-title-text sub-class">Simulasi Kredit Mobil Baru dan Cicilan Mobil Murah
</h1>
        <div class="input-section">
            <div class="input-groups select-cars-group">
                <label for="select-cars" class="input-label">Pilih Mobil</label>
                <?php
                populate_car_dropdown();
                ?>
            </div>
            <div class="input-groups car-price-group">
                <label for="car-price" class="input-label">Harga Mobil (Rp)</label>
                <input type="number" id="car-price" class="input-field" value="200000">
            </div>
            <div class="input-groups slider-group">
                <div class="label-input-wrapper">
                    <label for="down-payment" class="input-label">Uang Muka</label>
                    <div class="percentage-field-wrapper">
                        <input type="number" class="percentage-field" id="down-payment" value="30">
                        <span class="percentage-symbol">%</span>
                    </div>
                </div>
                <input type="range" style=" accent-color: black !important;" id="down-payment-slider" class="accent" min="1" max="100" value="30">
            </div>
            <div class="input-groups loan-period-group">
                <div class="label-input-wrapper">
                    <label for="loan-period" class="input-label">Siklus Pinjaman (tahun)</label>
                    <input type="number" id="loan-period" class="year-field " value="7">
                </div>
                <input type="range" style=" accent-color: black !important;" id="loan-period-slider" class="accent" min="1" max="10" value="7">
            </div>
           <div style="width:50%;">
			    <div class="input-groups interest-rate-group">
                <div class="label-input-wrapper">
                    <label for="interest-rate" class="input-label">Suku Bunga</label>
                    <div class="percentage-field-wrapper">
                        <input type="number" class="percentage-field" id="interest-rate" value="3">
                        <span class="percentage-symbol">%</span>
                    </div>
                </div>
                <input type="range" style=" accent-color: black !important;" id="interest-rate-slider" class="accent" min="0" max="10" value="3">
            </div>
			</div>
        </div>

        <div class="result-section">
            <div class="result-card-car-loan">
                <div class="car-loan-payment">
                    <h4 class="result-title">ชำระรายเดือน</h4>
                    <h2>THB 201</h2>
                </div>
                <p class="result-info">Uang Muka: </p>
                <p class="down-payment-summary result-value">THB 0</p>
                <hr>
                <p class="result-info">ค่าใช้จ่ายทั้งหมด:</p>
                <p class="total-cost-summary result-value" id="total-cost-value">THB 6,000</p>
            </div>
        </div>
    </div>

    <script>
        //calculate monthly paymment
        document.addEventListener('DOMContentLoaded', function() {
            console.log('event called');
            const carPriceInput = document.getElementById('car-price');
            const downPaymentInput = document.getElementById('down-payment');
            const loanPeriodInput = document.getElementById('loan-period');
            const interestRateInput = document.getElementById('interest-rate');
            const downPaymentSlider = document.querySelector("#down-payment-slider");
            const loanPeriodSlider = document.querySelector("#loan-period-slider");
            const interestRateSlider = document.querySelector("#interest-rate-slider");
            const monthlyPaymentElement = document.querySelector('.car-loan-payment h2');
            const downPaymentSummaryElement = document.querySelector('.down-payment-summary');
            const totalCostElement = document.getElementById('total-cost-value');

            function calculateDownPayment() {
                const carPrice = parseFloat(carPriceInput.value) || 0;
                const downPaymentPercentage = parseFloat(downPaymentInput.value) || 0;
                // const downPaymentAmount = (carPrice * downPaymentPercentage) / 100;
                const downPaymentAmount = Math.floor((carPrice * downPaymentPercentage) / 100);
                downPaymentSummaryElement.textContent = "THB " + downPaymentAmount.toLocaleString();

                return downPaymentAmount;
            }

            function calculateMonthlyPayment() {
                try {
                    const carPrice = parseFloat(carPriceInput.value) || 0;
                    const loanPeriodInYears = parseFloat(loanPeriodInput.value) || 0;
                    const interestRatePercentage = parseFloat(interestRateInput.value) / 100 || 0;
                    const downPaymentAmount = calculateDownPayment(); // Call down payment calculation
                    const loanAmount = carPrice - downPaymentAmount;
                    const loanPeriodInMonths = loanPeriodInYears * 12;
                    const totalInterest = loanAmount * interestRatePercentage * loanPeriodInYears;
                    const totalCost = Math.floor(carPrice + totalInterest);
                    totalCostElement.textContent = `THB ${totalCost.toLocaleString()}`;

                    const totalInstalment = loanAmount + totalInterest;
                    const monthlyPayment = totalInstalment / loanPeriodInMonths;
                    if (monthlyPayment > 0) {
                        monthlyPaymentElement.textContent = `THB ${Math.floor(monthlyPayment).toLocaleString()}`;
                        // Generate repayment schedule
                        generateRepaymentSchedule(loanPeriodInMonths, monthlyPayment, loanAmount, totalInterest);
                    } else {
                        monthlyPaymentElement.textContent = 'THB 0.00';
                        totalCostElement.textContent = 'THB 0.00';
                    }


                } catch (error) {
                    console.error('Error calculating monthly payment:', error);
                }
            }

            function generateRepaymentSchedule(loanPeriodInMonths, monthlyPayment, loanAmount, totalInterest) {
                const repaymentScheduleTbody = document.getElementById('repayment-schedule-tbody');
                repaymentScheduleTbody.innerHTML = '';
                let outstandingBalance = loanAmount;
                const monthlyInterestRate = totalInterest / loanPeriodInMonths;
                for (let i = 1; i <= loanPeriodInMonths; i++) {
                    const interestForThisMonth = monthlyInterestRate; // Interest for this month
                    const principalPayment = monthlyPayment - interestForThisMonth; // Calculate principal payment

                    outstandingBalance -= principalPayment;
                    outstandingBalance = Math.max(outstandingBalance, 0);
                    const row = document.createElement('tr');
                    row.innerHTML = `
                        <td>${i}</td>
                        <td>THB ${Math.floor(monthlyPayment).toLocaleString()}</td>
                        <td>THB ${Math.floor(outstandingBalance).toLocaleString()}</td>
                    `;
                    repaymentScheduleTbody.appendChild(row);
                }

                // Check if there are more than 5 rows
                const rows = repaymentScheduleTbody.rows;
                if (rows.length > 5) {
                    const tableBody = document.getElementById('repayment-schedule-tbody');
                    tableBody.style.width = '100%';
                }
            }

            // Event listeners for updating calculations on input changes
            carPriceInput.addEventListener('input', calculateMonthlyPayment);
            downPaymentInput.addEventListener('input', calculateMonthlyPayment);
            downPaymentSlider.addEventListener('input', function() {
                downPaymentInput.value = this.value;
                calculateMonthlyPayment();
            });
            loanPeriodInput.addEventListener('input', calculateMonthlyPayment);
            loanPeriodSlider.addEventListener('input', function() {
                loanPeriodInput.value = this.value;
                calculateMonthlyPayment();
            });
            interestRateInput.addEventListener('input', calculateMonthlyPayment);
            interestRateSlider.addEventListener('input', function() {
                interestRateInput.value = this.value;
                calculateMonthlyPayment();
            });

            // Initialize sliders with current values
            function updateSliderColors() {
                updateSliderColor(downPaymentSlider, downPaymentSlider.value);
                updateSliderColor(loanPeriodSlider, loanPeriodSlider.value);
                updateSliderColor(interestRateSlider, interestRateSlider.value);
            }

            function updateSliderColor(slider, value) {
                const max = slider.max;
                const percentage = (value / max) * 100;
                slider.style.setProperty("--slider-fill", `${percentage}%`);
            }

            // Initialize the color of sliders on page load
            updateSliderColors();
            calculateMonthlyPayment();


        });
    </script>
<?php
    return ob_get_clean();
}
add_shortcode('car_loan_calculator_ui', 'car_loan_calculator_shortcode');

function repayment_schedule_shortcode()
{
    ob_start();
?>
    <div class="repayment-schedule-section">
        <h2 class="wa-title-text">ตารางผ่อนรถ(เดือน)</h2>
        <div id="repayment-schedule-container">
            <table id="repayment-schedule-table">
                <thead class="wa-head-car-loan">
                    <tr>
                        <th>เดือน</th>
                        <th>ชำระรายเดือน</th>
                        <th>ยอดคงค้าง</th>
                    </tr>
                </thead>
                <tbody id="repayment-schedule-tbody">
                </tbody>
            </table>
        </div>
    </div>
    <style>
        /* table {
            max-width: 74%;
        } */
        .wa-head-car-loan {
            font-family: Roboto;
            font-size: 14px;
        }

        #repayment-schedule-table thead tr th {
            font-family: "Roboto";
            font-size: 14px;
            font-weight: 700;
            color: #262626;
        }

        .sub-class {
            position: absolute;
            top: -89px;
            left: 0;
        }

        #repayment-schedule-table {
            width: 100%;
            border-collapse: collapse;
        }

        #repayment-schedule-tbody {
            display: block;
            max-height: 300px;
            overflow-y: auto;
        }

        #repayment-schedule-table thead,
        #repayment-schedule-table tbody tr {
            display: table;
            width: 100%;
            table-layout: fixed;
        }

        #repayment-schedule-tbody::-webkit-scrollbar {
            width: 6px;
        }

        #repayment-schedule-tbody::-webkit-scrollbar-track {
            background: transparent;
        }

        #repayment-schedule-tbody::-webkit-scrollbar-thumb {
            background-color: rgba(0, 0, 0, 0.2);
            border-radius: 3px;
        }

        #repayment-schedule-tbody {
            scrollbar-width: none;
            scrollbar-color: rgba(0, 0, 0, 0.2) transparent;
        }
    </style>
<?php
    return ob_get_clean();
}
add_shortcode('repayment_schedule_ui', 'repayment_schedule_shortcode');

function  insurance_calculator_shortcode()
{
    ob_start();
?>
    <div class="custom-template-container">
        <h1 class="wa-title-text sub-class">Simulasi Asuransi Mobil</h1>

        <div class="input-section">
            <div class="input-group">
                <label for="select-cars">Pilih Mobil</label>
                <?php
                populate_car_dropdown();
                ?>
            </div>
            <div class="input-group">
                <label for="fuel-price">Harga Mobil (Rp)</label>
                <input type="text" id="insurance-car-price" value="20000">
            </div>
            <div class="input-group">
                <label for="coverage-type">Tipe Cakupan</label>
                <select id="coverage-type">
                    <option value="comprehensive">Comprehensive</option>
                    <option value="third-party">Total Loss Only</option>
                </select>
            </div>

            <div class="input-group">
                <label for="location">Lokasi</label>
                <select id="location">
                    <option value="penisular-malaysia">Sumatera dan Kepulauannya</option>
                    <option value="sabah-sarawak-labuan">Jakarta,Banten,dan Jabar</option>
                    <option value="sabah-sarawak-labuan">daerah lain</option>
                </select>
            </div>

         <div class="insurance-options">
             <span class="insurance-options-label">Jenis Asuransi mobil TLO</span>
  
  <div class="cal-check-box-con">
    <label class="check-box-con" >
      <input type="checkbox" name="insurance-type" value="Banjir termasuk Angin Topan">
      <span style="margin-left: 8px;">Floods including Hurricanes</span>
    </label>
    
    <label class="check-box-con">
      <input type="checkbox" name="insurance-type" value="Bumi dan Tsunami">
      <span style="margin-left: 8px;">Earthquake and Tsunami</span>
    </label>
    
    <label class="check-box-con">
      <input type="checkbox" name="insurance-type" value="Huru-hara dan Kerusuhan (SRCC)">
      <span style="margin-left: 8px;">Riots and Riots (SRCC)</span>
    </label>
    
    <label class="check-box-con">
      <input type="checkbox" name="insurance-type" value="Terorisme dan Sabotase">
      <span style="margin-left: 8px;">Terrorism and Sabotage</span>
    </label>
    
    <label class="check-box-con" >
      <input type="checkbox" name="insurance-type" value="Kecelakaan Diri untuk Penumpang">
      <span style="margin-left: 8px;">Personal Accident for Passengers</span>
    </label>
  </div>
</div>
        </div>
        <div class="result-section">
            <div class="result-card">
                <p>Pembayaran Asuransi Tahunan</p>
                <h2 id="yearly-insurance-payment">Rp 799</h2>
            </div>
        </div>
    </div>

    <script>
        document.addEventListener('DOMContentLoaded', function() {
            function calculatePremium() {
                let carPrice = parseFloat(document.getElementById('insurance-car-price').value);
                let coverageType = document.getElementById('coverage-type').value;
                let location = document.getElementById('location').value;
                let engineCapacity = document.getElementById('engine-capacity').value;
                let ncd = parseFloat(document.getElementById('no-claims-discount').value);

                // First RM1,000 car price premium lookup
                let combinedCriteria = location + '|' + coverageType + '|' + engineCapacity;
                let firstCarPricePremium = getPremiumForFirst1000(combinedCriteria);

                // Remaining car price calculation
                let remainingCarPrice = carPrice - 1000;
                let remainingCarPricePer1000 = remainingCarPrice / 1000;

                // Location-based premium per RM1,000
                let locationPremiumPer1000 = (location === 'penisular-malaysia') ? 26 : 20.30;

                // Remaining car price premium
                let remainingCarPricePremium = remainingCarPricePer1000 * locationPremiumPer1000;

                // Total premium
                let totalPremium = firstCarPricePremium + remainingCarPricePremium;

                // Apply NCD
                let discountAmount = (ncd / 100) * totalPremium;
                let finalPremium = totalPremium - discountAmount;

                // Update yearly insurance payment on UI
                document.getElementById('yearly-insurance-payment').textContent = 'Rp ' + Math.floor(finalPremium);
            }

            // Function to get premium for the first RM1,000 based on the criteria
            function getPremiumForFirst1000(combinedCriteria) {
                let premiumTable = {
                    'penisular-malaysia|comprehensive|0-1400': 273.80,
                    'penisular-malaysia|comprehensive|1401-1650': 305.50,
                    'penisular-malaysia|comprehensive|1651-2200': 339.10,
                    'sabah-sarawak-labuan|comprehensive|0-1400': 196.20,
                    'sabah-sarawak-labuan|comprehensive|1401-1650': 220.00,
                    'sabah-sarawak-labuan|comprehensive|1651-2200': 243.90,
                    'penisular-malaysia|third-party|0-1400': 120.60,
                    'penisular-malaysia|third-party|1401-1650': 135.00,
                    'penisular-malaysia|third-party|1651-2200': 151.20,
                    'sabah-sarawak-labuan|third-party|0-1400': 67.50,
                    'sabah-sarawak-labuan|third-party|1401-1650': 75.60,
                    'sabah-sarawak-labuan|third-party|1651-2200': 85.20
                };

                return premiumTable[combinedCriteria] || 0;
            }

            // Attach event listeners to input fields to trigger calculation
            document.getElementById('insurance-car-price').addEventListener('input', calculatePremium);
            document.getElementById('coverage-type').addEventListener('change', calculatePremium);
            document.getElementById('location').addEventListener('change', calculatePremium);
            document.getElementById('engine-capacity').addEventListener('change', calculatePremium);
            document.getElementById('no-claims-discount').addEventListener('change', calculatePremium);

            // Initial calculation on page load
            calculatePremium();
        });
    </script>
<?php
    return ob_get_clean();
}
add_shortcode('insurance_calculator_ui', 'insurance_calculator_shortcode');


function loan_calculator_tabs_shortcode()
{
    ob_start();
?>
    <div class="tab-container">

        <ul class="tabs">
             <li class="tab-head current" data-tab="tab-1">Simulasi Kredit Mobil</li>
            <li class="tab-head" data-tab="tab-2">Simulasi Asuransi Mobil</li>
            <li class="tab-head" data-tab="tab-3">Pajak Tahunan</li>
        </ul>

        <div id="tab-1" class="tab-content current">
            <?php echo do_shortcode('[car_loan_calculator_ui]'); ?>
            <div style="display: flex; position:relative;">
                <div style="width:72%">
                    <?php echo do_shortcode('[repayment_schedule_ui]'); ?>
                    <?php echo do_shortcode('[car_loan_data]'); ?>
                    <?php echo do_shortcode('[car_payment_faqs]'); ?>
                    <?php echo do_shortcode('[car_loan_intro]'); ?>
                </div>
                <div style="position: absolute; right:0px; top: 10px; width:25%;">
                    <?php echo do_shortcode('[buying_guides_shortcode]'); ?>
                    <?php echo do_shortcode('[elementor-template id="123247"]'); ?>

                </div>
            </div>
        </div>
        <div id="tab-2" class="tab-content">
            <?php echo do_shortcode('[insurance_calculator_ui]'); ?>
            <div style="display: flex; position:relative;">
                <div style="width:72%">
                    <?php echo do_shortcode('[road_tax_faqs]'); ?>
                    <?php echo do_shortcode('[road_tax_intro]'); ?>
                </div>
                <div style="position: absolute; right:0px; top: 10px; width:25%;">
                    <?php echo do_shortcode('[elementor-template id="123247"]'); ?>


                </div>
            </div>
        </div>
        <div id="tab-3" class="tab-content">
            <?php echo do_shortcode('[[road_tax_calculator_ui]]'); ?>
            <div style="display: flex; position:relative;">
                <div style="width:72%">
                    <?php echo do_shortcode('[fuel_cost_data]'); ?>
                    <?php echo do_shortcode('[fuel_cost_faqs]'); ?>

                    <div style="position: absolute; right:0px; top: 10px; width:25%;">
                        <?php echo do_shortcode('[elementor-template id="123247"]'); ?>
                    </div>
                </div>
            </div>
        </div>
        <div style="max-width: 72%;">
            <?php echo do_shortcode('[recommended_cars_horizontal]'); ?>
            <?php echo do_shortcode('[popular_car_brands]'); ?>
        </div>

        <style>
			.insurance-options{
				margin-top:20px;
			}
			.cal-check-box-con{
				margin-top:10px;
			}
			.insurance-options-label{
				font-family: Roboto;
				font-size: 14px;
				font-weight: 400;
				color: #262626;
				margin-bottom: 8px;
				line-height: 20px;
			}
			.cal-check-box-con{
				    display: flex;
				flex-wrap: wrap;
				gap: 12px;
			}
			.check-box-con{
				flex-basis: 240px;
				color: #606266;
				font-weight: 500;
				font-size: 14px;
				position: relative;
				cursor: pointer;
				white-space: nowrap;
			}
            /* Style for tabs */
            .tab-container {
                width: 100%;
                margin: 0 auto;
            }

            .tabs {
                display: flex;
                list-style: none;
                padding: 0;
                justify-content: flex-start;
            }

            .tabs li {
                padding: 10px 15px;
                font-weight: bold;
                color: #333;
                border-bottom: 3px solid transparent;
                cursor: pointer;
                font-size: 16px;
                font-family: "Roboto";
            }

            .tabs li.current {
                color: black;
                border-bottom: 3.5px solid #ffb400;
            }

            .tab-content {
                display: none;
                margin-top: -12px;
            }

            .tab-content.current {
                display: block;
            }
        </style>
        <script>
            document.addEventListener('DOMContentLoaded', function() {
                const tabs = document.querySelectorAll('.tab-head');
                const contents = document.querySelectorAll('.tab-content');

                function activateTab(tabId) {
                    tabs.forEach(t => t.classList.remove('current'));
                    document.querySelector(`[data-tab="${tabId}"]`).classList.add('current');

                    contents.forEach(c => c.classList.remove('current'));
                    document.getElementById(tabId).classList.add('current');
                    // initializeTabDropdown($("#" + tabId));
                }

                function handleTabActivation() {
                    const currentUrl = window.location.pathname;

                    if (currentUrl.includes('/tools/insurance-calculator')) {
                        activateTab('tab-2');
                    } else if (currentUrl.includes('/tools/road-tax-calculator')) {
                        activateTab('tab-3');
                    } else if (currentUrl.includes('/tools/fuel-cost-calculator')) {
                        activateTab('tab-4');
                    } else if (currentUrl.includes('/tools/loan-calculator')) {
                        activateTab('tab-1');
                    }
                }

                handleTabActivation();

                window.onpopstate = function(event) {
                    handleTabActivation();
                }

                tabs.forEach(tab => {
                    tab.addEventListener('click', function() {
                        const tabId = this.getAttribute('data-tab');
                        activateTab(tabId);
                    });
                });
            });
        </script>

    <?php
    return ob_get_clean();
}
add_shortcode('loan_calculator_tabs', 'loan_calculator_tabs_shortcode');

    ?>


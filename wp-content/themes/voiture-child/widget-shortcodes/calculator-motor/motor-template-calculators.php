<?php

function motor_populate_car_dropdown()
{
    global $wpdb;
//     $cache_key = 'motor_brands';
//     $car_brands = get_transient($cache_key);

//     if (false === $car_brands) {

        $car_brands = get_terms(array(
            'taxonomy'   => 'motorcycle_make',
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
            <div id="select-cars" class="btn-default dropdown-toggle multi-level-drop-down" data-toggle="dropdown">Pilihlah motor Anda</div>
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

function motor_car_loan_calculator_shortcode()
{
    ob_start();
?>
    <div class="custom-template-container">
        <h1 class="wa-title-text sub-class">Kredit Motor Murah dengan Cicilan Ringan</h1>
        <div class="input-section">
            <div class="input-groups select-cars-group">
                <label for="select-cars" class="input-label">Pilih Motor</label>
                <?php
                motor_populate_car_dropdown();
                ?>
            </div>
            <div class="input-groups car-price-group">
                <label for="car-price" class="input-label">Harga sepeda motor (Rp)</label>
                <input type="number" id="car-price" class="input-field" value="10000">
            </div>
            <div class="input-groups slider-group">
                <div class="label-input-wrapper">
                    <label for="down-payment" class="input-label">Uang Muka</label>
                    <div class="percentage-field-wrapper">
                        <input type="number" class="percentage-field" id="down-payment" value="10">
                        <span class="percentage-symbol">%</span>
                    </div>
                </div>
                <input type="range" style=" accent-color: black !important;" id="down-payment-slider" class="accent" min="1" max="100" value="10">
            </div>
            <div class="input-groups loan-period-group">
                <div class="label-input-wrapper">
                    <label for="loan-period" class="input-label">Siklus Pinjaman (bulan)</label>
                    <input type="number" id="loan-period" class="year-field " value="12">
                </div>
                <input type="range" style=" accent-color: black !important;" id="loan-period-slider" class="accent" min="1" max="10" value="12">
            </div>
			 <div style="width:50%;">
            <div class="input-groups interest-rate-group">
                <div class="label-input-wrapper">
                    <label for="interest-rate" class="input-label">Suku Bunga (bulan)
</label>
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
                    <h4 class="result-title">ชำระรายเดือน
                    </h4>
                    <h2>THB 1050</h2>
                </div>
                <p class="result-info">Uang Muka: </p>
                <p class="down-payment-summary result-value">THB 1,000</p>
                <hr>
                <p class="result-info">ค่าใช้จ่ายทั้งหมด:</p>
                <p class="total-cost-summary result-value" id="total-cost-value">THB 13,607</p>
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

            function motor_calculateDownPayment() {
                const carPrice = parseFloat(carPriceInput.value) || 0;
                const downPaymentPercentage = parseFloat(downPaymentInput.value) || 0;
                // const downPaymentAmount = (carPrice * downPaymentPercentage) / 100;
                const downPaymentAmount = Math.floor((carPrice * downPaymentPercentage) / 100);
                downPaymentSummaryElement.textContent = "THB " + downPaymentAmount;

                return downPaymentAmount;
            }

            function motor_calculateMonthlyPayment() {
                try {
                    const carPrice = parseFloat(carPriceInput.value) || 0;
                    const loanPeriodInYears = parseFloat(loanPeriodInput.value) || 0;
                    const interestRatePercentage = parseFloat(interestRateInput.value) / 100 || 0;
                    const downPaymentAmount = motor_calculateDownPayment(); // Call down payment calculation
                    const loanAmount = carPrice - downPaymentAmount;
                    const loanPeriodInMonths = loanPeriodInYears * 12;
                    const totalInterest = loanAmount * interestRatePercentage * loanPeriodInYears;
                    const totalCost = Math.floor(carPrice + totalInterest);
                    totalCostElement.textContent = `THB ${totalCost}`;

                    const totalInstalment = loanAmount + totalInterest;
                    const monthlyPayment = totalInstalment / loanPeriodInMonths;
                    if (monthlyPayment > 0) {
                        monthlyPaymentElement.textContent = `THB ${Math.floor(monthlyPayment)}`;
                        // Generate repayment schedule
                        motor_generateRepaymentSchedule(loanPeriodInMonths, monthlyPayment, loanAmount, totalInterest);
                    } else {
                        monthlyPaymentElement.textContent = 'THB 0.00';
                        totalCostElement.textContent = 'THB 0.00';
                    }


                } catch (error) {
                    console.error('Error calculating monthly payment:', error);
                }
            }

            function motor_generateRepaymentSchedule(loanPeriodInMonths, monthlyPayment, loanAmount, totalInterest) {
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
                        <td>THB ${Math.floor(monthlyPayment)}</td>
                        <td>THB ${Math.floor(outstandingBalance)}</td>
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
            carPriceInput.addEventListener('input', motor_calculateMonthlyPayment);
            downPaymentInput.addEventListener('input', motor_calculateMonthlyPayment);
            downPaymentSlider.addEventListener('input', function() {
                downPaymentInput.value = this.value;
                motor_calculateMonthlyPayment();
            });
            loanPeriodInput.addEventListener('input', motor_calculateMonthlyPayment);
            loanPeriodSlider.addEventListener('input', function() {
                loanPeriodInput.value = this.value;
                motor_calculateMonthlyPayment();
            });
            interestRateInput.addEventListener('input', motor_calculateMonthlyPayment);
            interestRateSlider.addEventListener('input', function() {
                interestRateInput.value = this.value;
                motor_calculateMonthlyPayment();
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
            motor_calculateMonthlyPayment();


        });
    </script>
<?php
    return ob_get_clean();
}
add_shortcode('motor_car_loan_calculator_ui', 'motor_car_loan_calculator_shortcode');

function motor_repayment_schedule_shortcode()
{
    ob_start();
?>
    <div class="repayment-schedule-section">
        <h2 class="wa-title-text">สินเชื่อรถจักรยานยนต์ที่คุณผ่อนชำระรายเดือน</h2>
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
add_shortcode('motor_repayment_schedule_ui', 'motor_repayment_schedule_shortcode');

function motor_loan_calculator_tabs_shortcode()
{
    ob_start();
?>
    <div class="tab-container">
        <?php echo do_shortcode('[motor_car_loan_calculator_ui]'); ?>
        <div style="display: flex; position:relative;">
            <div style="width:72%">
                <?php echo do_shortcode('[motor_repayment_schedule_ui]'); ?>
                <?php 
// 	echo do_shortcode('[motor_car_loan_data]'); 
				?>
                <?php echo do_shortcode('[motor_payment_faqs]'); ?>
                <?php echo do_shortcode('[motor_loan_intro]'); ?>
            </div>
            <div style="position: absolute; right:0px; top: 10px; width:25%;">
                <?php echo do_shortcode('[motor_buying_guides_shortcode]'); ?>
                <?php echo do_shortcode('[elementor-template id="123247"]'); ?>
            </div>
        </div>
        <div style="max-width: 72%;">
            <?php 
// 				echo do_shortcode('[recommended_bikes_horizontal]'); 
			?>
            <?php echo do_shortcode('[popular_bike_brands]'); ?>
        </div>

    <?php
    return ob_get_clean();
}
add_shortcode('motor_loan_calculator_tabs', 'motor_loan_calculator_tabs_shortcode');

    ?>
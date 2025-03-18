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
            <div id="select-cars" class="btn-default dropdown-toggle multi-level-drop-down" data-toggle="dropdown">Chọn xe của bạn
<span class="caret"></span></div>
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

function  insurance_calculator_shortcode()
{
    ob_start();
?>
    <div class="custom-template-container">
        <h1 class="wa-title-text sub-class">Bảo Hiểm Xe</h1>

        <div class="input-section">
            <div class="input-group">
                <label for="select-cars">Chọn xe</label>
                <?php
                populate_car_dropdown();
                ?>
            </div>
            <div class="input-group">
                <label for="fuel-price">Giá xe (VND)</label>
                <input type="text" id="insurance-car-price" value="300000">
            </div>

            <div class="input-group">
                <label for="engine-capacity">Tuổi xe</label>
                <select id="engine-capacity">
                    <option value="new" selected>Xe ô tô mới mua</option>
                    <option value="1-3">Xe ô tô sử dụng 1 - 3 năm</option>
                    <option value="3-6">Xe ô tô sử dụng 3 - 6 năm</option>
                    <option value="6-10">Xe ô tô sử dụng 6 - 10 năm</option>
                    <option value="above-10">Xe ô tô sử dụng trên 10 năm</option>
                </select>
            </div>
        </div>
        <div class="result-section">
            <div class="result-card">
                <p>Chi phí bảo hiểm hàng năm</p>
                <h2 id="yearly-insurance-payment">VND 4.980,7</h2>
            </div>
        </div>
    </div>

    <script>
        document.addEventListener('DOMContentLoaded', function () {
            function calculatePremium() {
                const carPrice = parseFloat(document.getElementById('insurance-car-price').value) || 0;
                const engineCapacity = document.getElementById('engine-capacity').value;

                // Base premium calculation based on car price and age
                const basePremium = getBasePremium(engineCapacity, carPrice);

                // Update yearly insurance payment on UI
                document.getElementById('yearly-insurance-payment').textContent = formatCurrency(basePremium);
            }

            function getBasePremium(carAge, carPrice) {
                // Base rates for different car age brackets
                const baseRates = {
                    'new': 0.015,       // 1.5% for new cars
                    '1-3': 0.013,      // 1.3% for cars 1-3 years old
                    '3-6': 0.010,      // 1.0% for cars 3-6 years old
                    '6-10': 0.007,     // 0.7% for cars 6-10 years old
                    'above-10': 0.005  // 0.5% for cars above 10 years old
                };

                const rate = baseRates[carAge] || 0.015;
                return carPrice * rate;
            }

            // Attach event listeners to input fields to trigger calculation
            document.getElementById('insurance-car-price').addEventListener('input', calculatePremium);
            document.getElementById('engine-capacity').addEventListener('change', calculatePremium);
			
            // Initial calculation on page load
            calculatePremium();
        });
    </script>
<?php
    return ob_get_clean();
}
add_shortcode('insurance_calculator_ui', 'insurance_calculator_shortcode');

function car_loan_calculator_shortcode()
{
    ob_start();
?>
    <div class="custom-template-container">
        <h1 class="wa-title-text sub-class">Mua Xe Trả Góp - Bảng Tính Chi Phí Mua Xe Ô Tô Trả Góp

</h1>
        <div class="input-section">
            <div class="input-groups select-cars-group">
                <label for="select-cars" class="input-label">Chọn xe</label>
                <?php
                populate_car_dropdown();
                ?>
            </div>
            <div class="input-groups car-price-group">
                <label for="car-price" class="input-label">Giá xe (VND)</label>
                <input type="number" id="car-price" class="input-field" value="200000">
            </div>
            <div class="input-groups slider-group">
                <div class="label-input-wrapper">
                    <label for="down-payment" class="input-label">Phí trả trước</label>
                    <div class="percentage-field-wrapper">
                        <input type="number" class="percentage-field" id="down-payment" value="30">
                        <span class="percentage-symbol">%</span>
                    </div>
                </div>
                <input type="range" style=" accent-color: black !important;" id="down-payment-slider" class="accent" min="1" max="100" value="30">
            </div>
            <div class="input-groups loan-period-group">
                <div class="label-input-wrapper">
                    <label for="loan-period" class="input-label">Thời gian cho vay (năm)</label>
                    <input type="number" id="loan-period" class="year-field " value="7">
                </div>
                <input type="range" style=" accent-color: black !important;" id="loan-period-slider" class="accent" min="1" max="10" value="7">
            </div>
           <div style="width:50%;">
			    <div class="input-groups interest-rate-group">
                <div class="label-input-wrapper">
                    <label for="interest-rate" class="input-label">Lãi suất</label>
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
                    <h4 class="result-title">Thanh toán hàng tháng</h4>
                    <h2>VND 201</h2>
                </div>
                <p class="result-info">Phí trả trước </p>
                <p class="down-payment-summary result-value">VND 0</p>
                <hr>
                <p class="result-info">Tổng chi phí</p>
                <p class="total-cost-summary result-value" id="total-cost-value">VND 6,000</p>
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
                downPaymentSummaryElement.textContent = formatCurrency(downPaymentAmount);

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
                    totalCostElement.textContent = `${formatCurrency(totalCost)}`;

                    const totalInstalment = loanAmount + totalInterest;
                    const monthlyPayment = totalInstalment / loanPeriodInMonths;
                    if (monthlyPayment > 0) {
                        monthlyPaymentElement.textContent = `${formatCurrency(monthlyPayment)}`;
                        // Generate repayment schedule
                        generateRepaymentSchedule(loanPeriodInMonths, monthlyPayment, loanAmount, totalInterest);
                    } else {
                        monthlyPaymentElement.textContent = 'VND  0.00';
                        totalCostElement.textContent = 'VND  0.00';
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
                        <td>${formatCurrency(monthlyPayment)}</td>
                        <td>${formatCurrency(outstandingBalance)}</td>
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
        <h2 class="wa-title-text">Bảng Tính Phí Trả Góp</h2>
        <div id="repayment-schedule-container">
            <table id="repayment-schedule-table">
                <thead class="wa-head-car-loan">
                    <tr>
                        <th>Dự Kiến</th>
                        <th>Trả Nợ</th>
                        <th>Chưa Thanh Toán</th>
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

function loan_calculator_tabs_shortcode()
{
    ob_start();
?>
    <div class="tab-container">

        <ul class="tabs">
             <li class="tab-head current" data-tab="tab-1">Mua Xe Trả Góp</li>
            <li class="tab-head" data-tab="tab-2">Bảo Hiểm Xe</li>
<!--             <li class="tab-head" data-tab="tab-3">Pajak Tahunan</li> -->
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
                    <?php echo do_shortcode('[insurance_faqs]'); ?>
                    <?php echo do_shortcode('[insurance_intro]'); ?>
                </div>
                <div style="position: absolute; right:0px; top: 10px; width:25%;">
                    <?php echo do_shortcode('[elementor-template id="123247"]'); ?>


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

                    if (currentUrl.includes('/dung-cu/bao-hiem-xe')) {
                        activateTab('tab-2');
                    } else if (currentUrl.includes('/dung-cu/mua-xe-tra-gop')) {
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
			
			// Function to format numbers as VND with Triệu or TY
			function formatCurrency(amount) {
				if (amount >= 1_000_000_000) {
					// Convert to TY (billions)
					return `${(amount / 1_000_000_000).toFixed(2)} TY`;
				} else if (amount >= 1_000_000) {
					// Convert to Triệu (millions)
					return `${(amount / 1_000_000).toFixed(2)} Triệu`;
				} else {
					// Default to VND
					return `VND ${amount.toLocaleString('vi-VN')}`;
				}
			}
        </script>

    <?php
    return ob_get_clean();
}
add_shortcode('loan_calculator_tabs', 'loan_calculator_tabs_shortcode');

    ?>


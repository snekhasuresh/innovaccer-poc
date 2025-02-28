<?php


function variant_top_widget_shortcode()
{




    ob_start();
?>
    <div class="container-variant">
        <div class="car-header">
            <h1 class="car-title">Honda HR-V</h1>
            <div class="variant-switch">
                <div class="dropdown">
                    <button class="btn btn-secondary dropdown-button" onclick="toggleDropdown()"><span style="margin-right: 5px;">Switch Variant</span> <i class="fas fa-chevron-down "></i> </button>
                    <div class="dropdown-menu" id="variantDropdown">
                        <a class="dropdown-item" href="#">Honda HR-V 1.5 S</a>
                        <a class="dropdown-item" href="#">Honda HR-V 1.8 V</a>
                        <a class="dropdown-item" href="#">Honda HR-V Hybrid</a>
                    </div>
                </div>
            </div>
        </div>
        <div class="container-card">
            <div class="row car-card">
                <!-- Car Image (5/12) -->
                <div class="col-md-5 car-image">
                    <img src="https://images.wapcar.my/file1/c3b799a0eef844cfa735e1c9422eb17d_912x516.jpg" alt="Honda HR-V">
                </div>


                <!-- Car Details (7/12) -->
                <div class="col-md-7 car-details position-relative">
                    <!-- Buttons Positioned at Top-Right -->
                    <div class="top-right-buttons">
                        <button class="calculate"><i class="fa-solid fa-square-root-variable" style="margin-right: 4px;"></i>Calculate</button>
                        <button class="compare">
                            <i class="fas fa-plus"></i> Compare
                        </button>


                    </div>


                    <div class="heading-name">
                        <h2 class="price">RM 115,900 <span class="monthly">RM 1,168/month</span></h2>
                    </div>
                    <p class="subtitle">2022 Honda HR-V 1.5 S Price In Malaysia</p>
                    <div class="row costs">
                        <div class="col-md-4">
                            <button class="btn btn-cost">
                                <div class="icon-div">
                                    <span class="icon">🛣️</span>


                                </div>
                                <div class="tax-div">


                                    <span>Road Tax Cost</span>


                                </div>
                                <div class=" amount-div">
                                    <span class="cost">RM 90.00/year</span>
                                </div>
                            </button>
                        </div>
                        <div class="col-md-4">
                            <button class="btn btn-cost">
                                <div class="icon-div">
                                    <span class="icon">☂️</span>


                                </div>
                                <div class="tax-div">
                                    <span>Insurance Cost</span>




                                </div>


                                <div class="amount-div">
                                    <span class="cost">RM 3,292.90/year</span>
                                </div>
                            </button>
                        </div>
                        <div class="col-md-4">
                            <button class="btn btn-cost">
                                <div class="icon-div">
                                    <span class="icon">⛽</span>


                                </div>
                                <div class="tax-div">


                                    <span>Fuel Cost</span>






                                </div>
                                <div class="amount-div">
                                    <span class="cost">RM 2,419.00/year</span>
                                </div>
                            </button>
                        </div>
                    </div>
                    <div class="instruction mt-3">
                        <span>* For reference only, you can adjust your real situation with the calculator.</span>
                    </div>


                    <div class="extras mt-5">
                        <button class=" view-specs">
                            View Specs
                        </button>
                        <button class=" trade-in">
                            Trade in for this car
                        </button>


                    </div>
                </div>
            </div>
        </div>
        <div class="tittle-table">
            <h2>Honda HR-V Specification</h2>


        </div>


        <div class="spec-table col-md-12    ">
            <div class=" col-md-6">
                <table>
                    <tr>
                        <td class="spec-label">Brand</td>
                        <td class="spec-value">Honda</td>
                    </tr>
                    <tr>
                        <td class="spec-label">Body Type</td>
                        <td class="spec-value">SUV</td>
                    </tr>
                    <tr>
                        <td class="spec-label">Launched Year</td>
                        <td class="spec-value">2022</td>
                    </tr>
                    <tr>
                        <td class="spec-label">Horsepower (ps)</td>
                        <td class="spec-value">121</td>
                    </tr>
                    <tr>
                        <td class="spec-label">Engine</td>
                        <td class="spec-value">1.5L 121PS</td>
                    </tr>
                    <tr>
                        <td class="spec-label">Length * Width * Height (mm)</td>
                        <td class="spec-value">4330 x 1790 x 1590</td>
                    </tr>
                </table>










            </div>
            <div class=" col-md-6">


                <table>
                    <tr>
                        <td class="spec-label">Model</td>
                        <td class="spec-value">Honda HR-V</td>


                    </tr>
                    <tr>
                        <td class="spec-label">Generation</td>
                        <td class="spec-value">RV</td>
                    </tr>
                    <tr>
                        <td class="spec-label">Assembly</td>
                        <td class="spec-value">-</td>
                    </tr>
                    <tr>
                        <td class="spec-label">Torque (Nm)</td>
                        <td class="spec-value">145</td>
                    </tr>
                    <tr>
                        <td class="spec-label">Transmission</td>
                        <td class="spec-value">CVT</td>
                    </tr>
                    <tr>
                        <td class="spec-label">0-100 km/h (s)</td>
                        <td class="spec-value">12.1</td>
                    </tr>
                </table>
            </div>
        </div>


    </div>










    <style>
        .compare {
            border: 1px solid #d9d9d9;
            width: 102px;
            color: black;
            padding: 8px;
            font-size: 14px;
            border-radius: 5px;
            margin-left: 10px;
            background: white;
            font-weight: 600;
        }


        .calculate {
            border: 1px solid #d9d9d9;
            width: 102px;
            color: black;
            padding: 8px;
            font-size: 14px;
            border-radius: 5px;
            margin-left: 10px;
            background: white;
            font-weight: 600;
        }


        .tittle-table {
            display: block;

            position: relative;
            font-size: 26px;
            line-height: 32px;
            padding: 8px 0;
        }


        .spec-table {
            max-width: 80% !important;
            background: #f9f9f9;
            padding: 8px 18px;
            border-radius: 4px;
            border: 1px solid #f5f5f5;
        }


        table {
            width: 100%;
            border-collapse: collapse;
            border: none !important;


        }


        tr {
            border: none !important;
        }


        tr {
            border-bottom: 1px solid #f0f0f0 !important;
        }


        td {
            padding: 12px 15px;
            border: none !important;
        }


        .spec-label {
            font-weight: 600;
            color: #a8a8a8;
            font-size: 16px;
        }


        .spec-value {
            color: #333;
            text-align: end;
            font-size: 16px;
            font-weight: 700;
        }


        .view-more {
            text-align: center;
            padding: 15px;
            display: flex;
            justify-content: center;
        }


        .view-more a {


            color: #0066cc;
            text-decoration: none;
        }






        .heading-name {
            display: flex;
            align-items: center;
        }


        .instruction {
            margin-top: 10px;
            color: #9d9d9d;
            font-size: 12px;
        }


        .container-card {
            width: 100%;
            margin-top: 20px;
        }


        .container-variant {
            width: 111%;
            margin-left: -98px;


        }


        .icon-div {
            display: flex;
            justify-content: flex-start;
        }


        .tax-div {
            display: flex;
            justify-content: flex-start;
        }


        .amount-div {
            display: flex;
            justify-content: flex-start;
        }






        .car-card {
            display: flex;
            margin-bottom: 20px;
            width: 100%;
        }


        .car-image {
            flex: 5;
            max-width: 100%;
        }


        .car-image img {
            width: 100%;
            border-radius: 2px;
        }


        .car-details {
            padding: 0px !important;
            position: relative;


        }


        .top-right-buttons {
            position: absolute;
            top: 0;
            right: 0;
            margin: 10px;
        }


        .top-right-buttons .btn {
            margin-left: 10px;
        }


        .price {
            font-size: 28px;
            color: #576b95;
            font-weight: 700;
            margin: 10px 0;
        }


        .price .monthly {
            font-size: 18px;
            color: #576b95;
            line-height: 20px;
            display: inline-block;
            background: rgba(87, 107, 149, .1);
            padding: 5px 8px;
            margin-left: 8px;
            border-radius: 4px;
        }


        .subtitle {
            color: #777;
        }


        .car-title {
            font-size: 26px !important;

            color: #262626;
            line-height: 32px;
        }


        .cost {


            font-size: 12px !important;
        }


        .car-header {
            display: flex;
            gap: 15px;
        }


        .btn-cost {
            display: flex;
            flex-direction: column;
            align-items: flex-start;
            justify-content: flex-start !important;
            background-color: #e9ecef !important;
            border: 1px solid #ddd;
            padding: 15px;
            width: 100%;
            height: 81px;
            transition: background-color 0.3s;


        }


        .btn-cost:hover {
            background-color: #e9ecef;
        }


        .icon {
            font-size: 24px;
            margin-bottom: 5px;
        }


        .cost {
            font-weight: bold;
        }


        .extras {


            display: flex;
            justify-content: space-between;
        }


        .trade-in {
            border: none;
            width: 320px;
            color: white;
            background-color: #00d1b2;
            padding: 10px;
            font-size: 14px;
            border-radius: 5px;
            font-weight: 700;
            margin-left: 10px;
        }


        .view-specs {
            border: 1px solid #00bfa5;
            width: 320px;
            color: #00bfa5;
            background-color: white;
            padding: 10px;
            font-size: 14px;
            border-radius: 5px;
            font-weight: 700;
        }


        .dropdown {
            position: relative;
            display: inline-block;


        }


        .dropdown-menu {
            display: none;
            position: absolute;
            background-color: #ffffff;
            min-width: 250px !important;
            border: 1px solid #ddd;
            box-shadow: 0 0 10px rgba(0, 0, 0, 0.2);
            z-index: 1;
            border-radius: 5px;
            font-size: 16px !important;
        }


        .dropdown-item {
            padding: 10px 15px;
            color: #333;
            text-decoration: none;
            display: block;
        }


        .dropdown-item:hover {
            background-color: #f0f0f0;
        }


        .dropdown-button {
            background-color: white !important;
            color: black !important;
            border: 1px solid #576b95 !important;
            padding: 7px 20px !important;
            border-radius: 5px !important;
            cursor: pointer !important;
            font-size: 14px !important;
            transition: background-color 0.3s !important;
            margin-top: 5px !important;
        }


        .variant-btn-custom:hover {
            background-color: #0056b3;
        }


        .specifications-container {
            width: 100%;
            max-width: 600px;
            margin: 0 auto;
            font-family: Arial, sans-serif;
        }


        h2 {
            font-size: 24px;
            margin-bottom: 20px;
        }


        .specs-table {
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 20px;
        }


        .specs-table td {
            padding: 10px;
            border: 1px solid #e0e0e0;
            text-align: left;
            font-size: 14px;
            color: #333;
        }


        .specs-table td strong {
            font-weight: 600;
            color: #000;
        }




        .view-more:hover {
            text-decoration: underline;
        }


        @media (max-width: 768px) {
            .specs-table td {
                font-size: 12px;
            }
        }
    </style>


    <script>
        function toggleDropdown() {
            var dropdown = document.getElementById("variantDropdown");
            dropdown.style.display = dropdown.style.display === "block" ? "none" : "block";
        }


        window.onclick = function(event) {
            if (!event.target.matches('.dropdown-button')) {
                var dropdown = document.getElementById("variantDropdown");
                if (dropdown.style.display === "block") {
                    dropdown.style.display = "none";
                }
            }
        }
    </script>


    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">






<?php
    return ob_get_clean();
}
add_shortcode('variant_top_widget_shortcode', 'variant_top_widget_shortcode');

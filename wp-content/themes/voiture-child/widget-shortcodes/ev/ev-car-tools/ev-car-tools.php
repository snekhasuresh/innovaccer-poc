<?php


function ev_car_tools()
{

    $home_url = get_home_url();
    $tools = [
        [
            "name" => "Sell Car",
            "icon" => "fas fa-car",
            "link" => "https://www.carsome.my/sell-car?utm_source=wapcar&utm_medium=partner&utm_campaign=my-c2b-en-conv-private_seller"
        ],
        [
            "name" => "Trade-in",
            "icon" => "fas fa-exchange-alt",
            "link" => home_url() . "/tools/trade-in-your-car"
        ],
        [
            "name" => "Insurance",
            "icon" => "fas fa-shield-alt",
            "link" => home_url() . "/tools/insurance-calculator"
        ],
        [
            "name" => "Car Loan Calculator",
            "icon" => "fas fa-calculator",
            "link" => home_url() . "/tools/loan-calculator"
        ],
        [
            "name" => "My Car",
            "icon" => "fas fa-car",
            "link" => home_url() . "/car-owner-service"
        ],
        [
            "name" => "Car Comparison",
            "icon" => "fas fa-compare",
            "link" => home_url() . "/compare-cars"
        ]
    ];


    ob_start();
?>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
    <h2 class="wa-title-text">EV Car Tools</h2>
    <div class="container-tools">
        <div class="tools-grid">
            <?php foreach ($tools as $tool) : ?>
                <div class="tool-item">
                    <a href="<?php echo $tool['link']; ?>" class="tool-link">
                        <i class="<?php echo $tool['icon']; ?>"></i>
                        <p><?php echo $tool['name']; ?></p>
                    </a>
                </div>
            <?php endforeach; ?>
        </div>
    </div>


    <style>
        .container-tools {
            max-width: 100%;
            box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1);
            border-radius: 5px;
        }

        h2 {
            margin-bottom: 20px;
        }

        .tools-grid {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
        }

        .tool-item {
            background: white;
            align-items: center;
            border-bottom: 1px solid #f0f0f0;
            border-right: 1px solid #f0f0f0;
            box-sizing: border-box;
            cursor: pointer;
            padding: 14px 12px;
        }

        .tool-link {
            text-decoration: none;
            color: #262626;
            display: flex;
            flex-direction: column;
            align-items: center;
            font-size: 12px;
        }

        .tool-link i {
            font-size: 40px;
            width: 40px;
            height: 40px;
            color: #262626;
            margin-bottom: 10px;
        }

        .tool-link p {
            margin: 0;
            color: #262626;
            font-size: 12px;
        }
    </style>


<?php
    return ob_get_clean();
}


add_shortcode('ev_car_tools', 'ev_car_tools');

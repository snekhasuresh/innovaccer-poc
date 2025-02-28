<?php
function related_model_shortcode($atts)
{
    $atts = shortcode_atts(
        array(
            'name' => 'Mitsubishi Xpander',
            'price' => 'RM 99,980',
            'image_url' => 'https://images.wapcar.my/file1/63e7e7faf88d419da99d31ab18354af6_606x402.jpg'
        ),
        $atts,
        'related_model'
    );

    ob_start();
?>
    <style>
        .title {
            display: block;

            position: relative;
            font-size: 25px;
            line-height: 32px;
            padding: 8px 0;
            font-weight: 700;
            color: #262626;
        }

        .card-demo {
            border: 1px solid #eee;
            border-radius: 8px;
            overflow: hidden;
            background: white;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            position: sticky;
            top: 20px;
            z-index: 100;
        }

        .car-image {
            width: 100%;
            height: 150px;
            background: #f5f5f5;
            display: block;
        }

        .car-info {
            padding: 15px;
        }

        .car-name {
            font-size: 16px;
            font-weight: bold;
            color: #262626;
            font-family: "Roboto";
            line-height: 22px;
            word-break: break-word;
            text-overflow: ellipsis;
            display: -webkit-box;
            -webkit-box-orient: vertical;
            -webkit-line-clamp: 1;
            overflow: hidden;
        }

        .car-price {
            font-size: 14px;
            line-height: 20px;
            height: 20px;
            font-weight: 700;
            font-family: "Roboto";
            color: #576b95;
        }

        .trade-button {
            background-color: #32d0c6;
            color: #fff;
            border: none;
            width: 100%;
            border-radius: 4px;
            height: 40px;
            cursor: pointer;
            line-height: 20px;
            font-size: 14px;
            margin-top: 15px;
        }

        .trade-button:hover {
            background: #3BC9BB;
        }

        .dropdown-toggle {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding-top: 5px;
            padding-left: 12px;
            padding-right: 12px;
            padding-bottom: 12px;
            cursor: pointer;
        }

        .model-list {
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.3s ease-out;
        }

        .model-list.active {
            max-height: 500px;
        }

        .model-list a {
            display: block;
            padding-top: 5px;
            padding-left: 15px;
            padding-bottom: 6px;
            text-decoration: none;
            color: #576b95;
            font-size: 13px;
        }

        .arrow {
            border: solid #666;
            border-width: 0 2px 2px 0;
            display: inline-block;
            padding: 3px;
            transform: rotate(45deg);
            transition: transform 0.3s;
        }

        .arrow.up {
            transform: rotate(-135deg);
        }

        .container-demo {
            width: 280px;
        }

        .image-con {
            width: 270px;
            height: 170px;
        }
    </style>
    <div class="container-demo">
        <span class="title">Related Models</span>
        <div class="card-demo">
            <div class="image-con">
                <img src="<?php echo esc_url($atts['image_url']); ?>" alt="<?php echo esc_attr($atts['name']); ?>" class="car-image">
            </div>
            <div class="car-info">
                <span class="car-name"><?php echo esc_html($atts['name']); ?></span>
                <span class="car-price"><?php echo esc_html($atts['price']); ?></span>
                <button class="trade-button">Trade in for this car</button>
            </div>
            <div class="dropdown-toggle" id="dropdown-toggle">
                <span>7 other Mitsubishi models</span>
                <span class="arrow"></span>
            </div>
            <div class="model-list" id="model-list">
                <a href="#">Mitsubishi ASX</a>
                <a href="#">Mitsubishi Attrage</a>
                <a href="#">Mitsubishi Lancer</a>
                <a href="#">Mitsubishi Mirage</a>
                <a href="#">Mitsubishi Outlander</a>
                <a href="#">Mitsubishi Triton</a>
                <a href="#">Mitsubishi Xforce</a>
            </div>
        </div>
    </div>
    <script>
        document.addEventListener('DOMContentLoaded', function() {
            const toggle = document.getElementById('dropdown-toggle');
            const modelList = document.getElementById('model-list');
            const arrow = toggle.querySelector('.arrow');

            toggle.addEventListener('click', function() {
                modelList.classList.toggle('active');
                arrow.classList.toggle('up');
            });
        });
    </script>
<?php
    return ob_get_clean();
}
add_shortcode('related_model', 'related_model_shortcode');

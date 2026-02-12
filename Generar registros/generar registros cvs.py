import csv
import random
import os
import sys
from datetime import datetime, timedelta

# Configuración de ruta
try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))
except:
    SCRIPT_DIR = os.getcwd()

# --- CONFIGURACIÓN ---
START_DATE = datetime(2020, 1, 1)
END_DATE = datetime(2025, 12, 31)

PROVINCIAS = ["Buenos Aires", "Córdoba", "Santa Fe", "Mendoza", "Tucumán"]
LOCALIDADES = ["Centro", "Norte", "Sur", "Oeste", "Este"]
PROVEEDORES = ["QuickSend", "Logística Express", "Envios Nacionales", "Flash Delivery"]
ALMACENES_LIST = [
    (1, "Centro Norte", "Rosario"),
    (2, "Hub Logístico", "Buenos Aires"),
    (3, "Centro Oeste", "Mendoza"),
    (4, "Depósito Sur", "Neuquén")
]

# --- FUNCIÓN DE GUARDADO CORREGIDA ---

def save_csv(filename, headers, data):
    full_path = os.path.join(SCRIPT_DIR, filename)
    with open(full_path, 'w', newline='', encoding='utf-8') as f:
        # extrasaction='ignore' hace que si el dict tiene 'IDVenta' pero headers no, no se rompe
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(data)
    print(f" Archivo creado: {filename} ({len(data)} registros)")

# --- GENERADORES DE DIMENSIONES ---

def gen_clientes():
    headers = ['CodCliente', 'RazonSocial', 'Telefono', 'Mail', 'Direccion', 'Localidad', 'Provincia', 'CP']
    data = []
    for i in range(1, 101):
        data.append({
            'CodCliente': i,
            'RazonSocial': random.choice(['ACME Corp', 'Globex', 'Dunder Mifflin', 'Wayne Ent.']) + f" {i}",
            'Telefono': f"549{random.randint(11111111, 99999999)}",
            'Mail': f"user{i}@empresa.com",
            'Direccion': f"Calle Ficticia {random.randint(1, 999)}",
            'Localidad': random.choice(LOCALIDADES),
            'Provincia': random.choice(PROVINCIAS),
            'CP': random.randint(1000, 9000)
        })
    return data, headers

def gen_productos():
    headers = ['CodigoProducto', 'Descripcion', 'Categoria', 'Marca', 'PrecioCosto', 'PrecioVentaSugerido']
    categorias = ["Electrodomésticos", "Tecnología", "Hogar"]
    marcas = ["LG", "Samsung", "Sony", "Philips"]
    data = []
    for i in range(1, 51):
        costo = round(random.uniform(50, 1500), 2)
        # Error para simular error de carga real: Precio negativo en el producto 13
        venta = round(costo * 1.3, 2) if i != 13 else -150.0 
        data.append({
            'CodigoProducto': i,
            'Descripcion': f"Producto Modelo {i}",
            'Categoria': random.choice(categorias),
            'Marca': random.choice(marcas),
            'PrecioCosto': costo,
            'PrecioVentaSugerido': venta
        })
    return data, headers

def gen_tiendas():
    headers = ['CodigoTienda', 'Descripcion', 'Direccion', 'Localidad', 'Provincia', 'CP', 'TipoTienda']
    data = []
    tipos = ["Sucursal", "Online", "Outlet"]
    for i in range(1, 11):
        data.append({
            'CodigoTienda': i,
            'Descripcion': f"Tienda {random.choice(tipos)} {i}",
            'Direccion': f"Av. Principal {random.randint(100, 2000)}",
            'Localidad': random.choice(LOCALIDADES),
            'Provincia': random.choice(PROVINCIAS),
            'CP': random.randint(1000, 9000),
            'TipoTienda': random.choice(tipos)
        })
    return data, headers

# --- GENERADOR DE HECHOS Y ENTREGAS ---

def gen_ventas_y_entregas(clientes, productos, tiendas):
    # headers finales de los CSV
    v_headers = ['FechaVenta', 'CodigoProducto', 'Producto', 'Cantidad', 'PrecioVenta', 'CodigoCliente', 'Cliente', 'CodigoTienda', 'Tienda']
    e_headers = ['CodEntrega', 'CodVenta', 'CodProveedor', 'Proveedor', 'CodAlmacen', 'Almacen', 'CodEstado', 'Estado', 'Fecha_Envio', 'Fecha_Entrega']
    
    ventas = []
    entregas = []
    
    id_venta_seq = 1
    id_entrega_seq = 1
    estados = {1: "En preparación", 2: "En tránsito", 3: "Entregado", 4: "Devuelto"}
    
    curr_date = START_DATE
    while curr_date <= END_DATE:
        for _ in range(random.randint(5, 15)):
            prod = random.choice(productos)
            cli = random.choice(clientes)
            tnd = random.choice(tiendas)
            
            venta_fecha = curr_date + timedelta(hours=random.randint(9, 20))
            
            venta_row = {
                'IDVenta': id_venta_seq, # Se usa para vincular pero save_csv lo ignorará al guardar
                'FechaVenta': venta_fecha.strftime("%Y-%m-%d"),
                'CodigoProducto': prod['CodigoProducto'],
                'Producto': prod['Descripcion'],
                'Cantidad': random.randint(1, 3),
                'PrecioVenta': prod['PrecioVentaSugerido'],
                'CodigoCliente': cli['CodCliente'],
                'Cliente': cli['RazonSocial'],
                'CodigoTienda': tnd['CodigoTienda'],
                'Tienda': tnd['Descripcion']
            }
            ventas.append(venta_row)
            
            # 70% de las ventas generan entrega (el resto es retiro presencial)
            if random.random() < 0.7:
                alm = random.choice(ALMACENES_LIST)
                cod_est = random.choice(list(estados.keys()))
                
                f_envio = venta_fecha + timedelta(days=random.randint(1, 3))
                f_entrega = f_envio + timedelta(days=random.randint(1, 5))
                
                # ERROR 1: Fecha entrega < Venta (Venta ID 50)
                if id_venta_seq == 50:
                    f_entrega = venta_fecha - timedelta(days=10)
                
                # ERROR 2: Fecha inexistente (Venta ID 75)
                envio_str = f_envio.strftime("%Y-%m-%d") if id_venta_seq != 75 else "2024-02-31"

                entregas.append({
                    'CodEntrega': id_entrega_seq,
                    'CodVenta': id_venta_seq,
                    'CodProveedor': random.randint(1, 4),
                    'Proveedor': random.choice(PROVEEDORES),
                    'CodAlmacen': alm[0],
                    'Almacen': alm[1],
                    'CodEstado': cod_est,
                    'Estado': estados[cod_est],
                    'Fecha_Envio': envio_str,
                    'Fecha_Entrega': f_entrega.strftime("%Y-%m-%d")
                })
                id_entrega_seq += 1
            
            id_venta_seq += 1
        curr_date += timedelta(days=1)
        
    return ventas, v_headers, entregas, e_headers

# --- EJECUCIÓN PRINCIPAL ---

if __name__ == "__main__":
    clientes_data, c_h = gen_clientes()
    productos_data, p_h = gen_productos()
    tiendas_data, t_h = gen_tiendas()
    
    all_ventas, v_h, entregas_data, e_h = gen_ventas_y_entregas(clientes_data, productos_data, tiendas_data)
    
    # Separar Ventas (2020-2024) y Ventas_add (2025)
    ventas_main = [v for v in all_ventas if not v['FechaVenta'].startswith('2025')]
    ventas_add = [v for v in all_ventas if v['FechaVenta'].startswith('2025')]
    
    # Datos estáticos
    estados_data = [
        {'CodEstado': 1, 'Descripcion_Estado': 'En preparación'},
        {'CodEstado': 2, 'Descripcion_Estado': 'En tránsito'},
        {'CodEstado': 3, 'Descripcion_Estado': 'Entregado'},
        {'CodEstado': 4, 'Descripcion_Estado': 'Devuelto'}
    ]
    
    almacenes_data = [{'CodAlmacen': a[0], 'Nombre_Almacen': a[1], 'Ubicacion': a[2]} for a in ALMACENES_LIST]

    # Guardar archivos
    save_csv('Clientes.csv', c_h, clientes_data)
    save_csv('Productos.csv', p_h, productos_data)
    save_csv('Tiendas.csv', t_h, tiendas_data)
    save_csv('Ventas.csv', v_h, ventas_main)
    save_csv('Ventas_add.csv', v_h, ventas_add)
    save_csv('EstadoDelPedido.csv', ['CodEstado', 'Descripcion_Estado'], estados_data)
    save_csv('Entregas.csv', e_h, entregas_data)
    save_csv('Almacenes.csv', ['CodAlmacen', 'Nombre_Almacen', 'Ubicacion'], almacenes_data)

    print("\n ¡Listo! Todos los archivos generados sin errores.")